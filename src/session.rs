use crate::ddp::DDPMessage;
use crate::inflights::{Inflight, Inflights};
use crate::mergebox::Mergebox;
use crate::subscription::Subscription;
use anyhow::{anyhow, Error};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use mongodb::Database;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const OK: Result<(), Error> = Ok(());

async fn process_message_client(
    ddp_message: DDPMessage,
    inflights: &Arc<Mutex<Inflights>>,
    mergebox: &Arc<Mutex<Mergebox>>,
    client_writer: &Sender<DDPMessage>,
    server_writer: &Sender<DDPMessage>,
    subscriptions: &Arc<Mutex<BTreeMap<String, Subscription>>>,
) -> Result<(), Error> {
    match ddp_message {
        // Intercept a client subscription into a router-managed subscription.
        DDPMessage::Sub { id, name, params } => {
            server_writer
                .send(DDPMessage::Method {
                    id: id.clone(),
                    method: format!("__subscription__{name}"),
                    params: params.clone(),
                    random_seed: None,
                })
                .await?;
            inflights
                .lock()
                .await
                .register(id, Inflight::new(name, params));
            OK
        }

        // Intercept a client unsubscription of a router-managed subscription.
        DDPMessage::Unsub { ref id } => {
            if let Some(subscription) = subscriptions.lock().await.remove(id) {
                let id = subscription.stop(mergebox).await?;
                client_writer
                    .send(DDPMessage::Nosub { id, error: None })
                    .await?;
                return OK;
            }

            server_writer.send(ddp_message).await?;
            OK
        }

        _ => {
            server_writer.send(ddp_message).await?;
            OK
        }
    }
}

async fn process_message_server(
    ddp_message: DDPMessage,
    database: &Database,
    inflights: &Arc<Mutex<Inflights>>,
    mergebox: &Arc<Mutex<Mergebox>>,
    client_writer: &Sender<DDPMessage>,
    server_writer: &Sender<DDPMessage>,
    subscriptions: &Arc<Mutex<BTreeMap<String, Subscription>>>,
) -> Result<(), Error> {
    match ddp_message {
        // Hide router method calls.
        DDPMessage::Result {
            ref id,
            ref error,
            ref result,
        } => {
            let mut inflights = inflights.lock().await;
            if let Some(inflight) = inflights.process_result(id) {
                let parsed_subscription = match (error, result) {
                    (Some(error), _) => match error.get("reason") {
                        Some(Value::String(message))
                            if message
                                == &format!(
                                    "Method '__subscription__{}' not found",
                                    inflight.name
                                ) =>
                        {
                            Err(anyhow!(
                                "Publication for {} was not registered",
                                inflight.name
                            ))
                        }
                        _ => Err(anyhow!(error.clone())),
                    },
                    (None, Some(Value::Array(cursor_descriptions))) => {
                        Subscription::try_from((database, id, cursor_descriptions))
                    }
                    (None, _) => Err(anyhow!("Incorrect format")),
                };

                match parsed_subscription {
                    Ok(mut subscription) => {
                        // If the method succeeded and returned only
                        // supported cursor descriptions, register them as
                        // router-managed subscription.
                        subscription.start(mergebox).await?;
                        client_writer
                            .send(DDPMessage::Ready {
                                subs: vec![subscription.id.clone()],
                            })
                            .await?;
                        subscriptions.lock().await.insert(id.clone(), subscription);
                    }
                    Err(error) => {
                        // If the method failed, did not provide a response,
                        // used an incorrect format, or requires an
                        // unsupported query option, start a classic server
                        // subscription instead.
                        println!("\x1b[0;31m{error}\x1b[0m");
                        server_writer
                            .send(DDPMessage::Sub {
                                id: id.clone(),
                                name: inflight.name,
                                params: inflight.params,
                            })
                            .await?;
                    }
                }

                return OK;
            }

            client_writer.send(ddp_message).await?;
            OK
        }

        DDPMessage::Updated { mut methods } => {
            let mut inflights = inflights.lock().await;
            methods.retain(|id| !inflights.process_update(id));
            if methods.is_empty() {
                return OK;
            }

            client_writer.send(DDPMessage::Updated { methods }).await?;
            OK
        }

        // Track server subscriptions in mergebox.
        DDPMessage::Added {
            id,
            collection,
            fields,
        } => {
            mergebox
                .lock()
                .await
                .server_added(collection, id, fields)
                .await
        }

        DDPMessage::Changed {
            id,
            collection,
            fields,
            cleared,
        } => {
            mergebox
                .lock()
                .await
                .server_changed(collection, id, fields, cleared)
                .await
        }

        DDPMessage::Removed { id, collection } => {
            mergebox.lock().await.server_removed(collection, id).await
        }

        // Pass-through other DDP messages.
        _ => {
            client_writer.send(ddp_message).await?;
            OK
        }
    }
}

async fn start_consumer_client(
    mut reader: Receiver<DDPMessage>,
    mut sink: SplitSink<WebSocketStream<TcpStream>, Message>,
) -> Result<(), Error> {
    while let Some(ddp_message) = reader.recv().await {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;34mclient\x1b[0m {ddp_message:?}");
        sink.send(ddp_message.try_into()?).await?;
    }

    OK
}

async fn start_consumer_server(
    mut reader: Receiver<DDPMessage>,
    mut sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> Result<(), Error> {
    while let Some(ddp_message) = reader.recv().await {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;36mserver\x1b[0m {ddp_message:?}");
        sink.send(ddp_message.try_into()?).await?;
    }

    OK
}

async fn start_producer_client(
    mut stream: SplitStream<WebSocketStream<TcpStream>>,
    inflights: Arc<Mutex<Inflights>>,
    mergebox: Arc<Mutex<Mergebox>>,
    client_writer: Sender<DDPMessage>,
    server_writer: Sender<DDPMessage>,
    subscriptions: Arc<Mutex<BTreeMap<String, Subscription>>>,
) -> Result<(), Error> {
    while let Some(raw_message) = stream.try_next().await? {
        let ddp_message = DDPMessage::try_from(raw_message)?;
        println!("\x1b[0;34mclient\x1b[0m -> \x1b[0;33mrouter\x1b[0m {ddp_message:?}");
        process_message_client(
            ddp_message,
            &inflights,
            &mergebox,
            &client_writer,
            &server_writer,
            &subscriptions,
        )
        .await?;
    }

    OK
}

async fn start_producer_server(
    mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    database: Database,
    inflights: Arc<Mutex<Inflights>>,
    mergebox: Arc<Mutex<Mergebox>>,
    client_writer: Sender<DDPMessage>,
    server_writer: Sender<DDPMessage>,
    subscriptions: Arc<Mutex<BTreeMap<String, Subscription>>>,
) -> Result<(), Error> {
    while let Some(raw_message) = stream.try_next().await? {
        let ddp_message = DDPMessage::try_from(raw_message)?;
        println!("\x1b[0;36mserver\x1b[0m -> \x1b[0;33mrouter\x1b[0m {ddp_message:?}");
        process_message_server(
            ddp_message,
            &database,
            &inflights,
            &mergebox,
            &client_writer,
            &server_writer,
            &subscriptions,
        )
        .await?;
    }

    OK
}

pub async fn start_session(
    database: Database,
    client: WebSocketStream<TcpStream>,
    server: WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), Error> {
    let mut tasks = JoinSet::new();

    // Setup websockets queues and communication.
    let (client_sink, client_stream) = client.split();
    let (server_sink, server_stream) = server.split();

    let (client_writer, client_reader) = channel::<DDPMessage>(64);
    let (server_writer, server_reader) = channel::<DDPMessage>(64);

    tasks.spawn(start_consumer_client(client_reader, client_sink));
    tasks.spawn(start_consumer_server(server_reader, server_sink));

    // Setup Mergebox.
    let mergebox = Arc::new(Mutex::new(Mergebox::new(client_writer.clone())));
    let subscriptions = Arc::new(Mutex::new(BTreeMap::new()));

    // Setup communication handlers.
    let inflights = Arc::new(Mutex::new(Inflights::default()));
    tasks.spawn(start_producer_client(
        client_stream,
        inflights.clone(),
        mergebox.clone(),
        client_writer.clone(),
        server_writer.clone(),
        subscriptions.clone(),
    ));
    tasks.spawn(start_producer_server(
        server_stream,
        database,
        inflights,
        mergebox,
        client_writer,
        server_writer,
        subscriptions,
    ));

    // Stop when any task's finished.
    // (It's safe to `unwrap` here, as we know there are tasks there.)
    tasks.join_next().await.unwrap()??;
    tasks.shutdown().await;
    OK
}
