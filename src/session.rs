use crate::ddp::DDPMessage;
use crate::inflights::{Inflight, Inflights};
use crate::mergebox::Mergebox;
use crate::subscriptions::Subscriptions;
use anyhow::{Context, Error};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

struct Session {
    id: usize,
    client_writer: Sender<DDPMessage>,
    server_writer: Sender<DDPMessage>,
    inflights: Mutex<Inflights>,
    mergebox: Arc<Mutex<Mergebox>>,
    subscriptions: Arc<Mutex<Subscriptions>>,
}

async fn process_message_client(session: &Session, ddp_message: DDPMessage) -> Result<(), Error> {
    match ddp_message {
        // Intercept a client subscription into a router-managed subscription.
        DDPMessage::Sub { id, name, params } => {
            // If we already checked this subscription and failed, pass it to
            // the server immediately.
            let is_server_subscription = session
                .subscriptions
                .lock()
                .await
                .is_server_subscription(&name);

            if is_server_subscription {
                session
                    .server_writer
                    .send(DDPMessage::Sub { id, name, params })
                    .await?;
            } else {
                session
                    .server_writer
                    .send(DDPMessage::Method {
                        id: id.clone(),
                        method: format!("__subscription__{name}"),
                        params: params.clone(),
                        random_seed: None,
                    })
                    .await?;
                session
                    .inflights
                    .lock()
                    .await
                    .register(id, Inflight::new(name, params));
            }

            Ok(())
        }

        // Intercept a client unsubscription of a router-managed subscription.
        DDPMessage::Unsub { ref id } => {
            if let Some(id) = session
                .subscriptions
                .lock()
                .await
                .stop(session.id, &session.mergebox, id)
                .await?
            {
                session
                    .client_writer
                    .send(DDPMessage::Nosub { id, error: None })
                    .await?;
            } else {
                session.server_writer.send(ddp_message).await?;
            }

            Ok(())
        }

        _ => {
            session.server_writer.send(ddp_message).await?;
            Ok(())
        }
    }
}

async fn process_message_server(session: &Session, ddp_message: DDPMessage) -> Result<(), Error> {
    match ddp_message {
        // Hide router method calls.
        DDPMessage::Result {
            ref id,
            ref error,
            ref result,
        } => {
            let mut inflights = session.inflights.lock().await;
            let Some(inflight) = inflights.process_result(id) else {
                session.client_writer.send(ddp_message).await?;
                return Ok(());
            };

            let subscription_started = session
                .subscriptions
                .lock()
                .await
                .start(session.id, &session.mergebox, &inflight, id, error, result)
                .await;

            match subscription_started {
                Ok(()) => {
                    // If the method succeeded and returned only supported
                    // cursor descriptions, register them as router-managed
                    // subscription.
                    let subs = vec![id.clone()];
                    session
                        .client_writer
                        .send(DDPMessage::Ready { subs })
                        .await?;
                }
                Err(error) => {
                    // If the method failed, did not provide a response, used an
                    // incorrect format, or requires an unsupported query
                    // option, start a classic server subscription instead.
                    println!("\x1b[0;31m[[ERROR]] {error:?}\x1b[0m");
                    session
                        .server_writer
                        .send(DDPMessage::Sub {
                            id: id.clone(),
                            name: inflight.name.clone(),
                            params: inflight.params,
                        })
                        .await?;
                }
            }

            Ok(())
        }

        DDPMessage::Updated { mut methods } => {
            let mut inflights = session.inflights.lock().await;
            methods.retain(|id| !inflights.process_update(id));
            if methods.is_empty() {
                return Ok(());
            }

            session
                .client_writer
                .send(DDPMessage::Updated { methods })
                .await?;
            Ok(())
        }

        // Track server subscriptions in mergebox.
        DDPMessage::Added {
            id,
            collection,
            fields,
            ..
        } => {
            session
                .mergebox
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
            session
                .mergebox
                .lock()
                .await
                .server_changed(collection, id, fields, cleared)
                .await
        }

        DDPMessage::Removed { id, collection } => {
            session
                .mergebox
                .lock()
                .await
                .server_removed(collection, id)
                .await
        }

        // Pass-through other DDP messages.
        _ => {
            session.client_writer.send(ddp_message).await?;
            Ok(())
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

    Ok(())
}

async fn start_consumer_server(
    mut reader: Receiver<DDPMessage>,
    mut sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> Result<(), Error> {
    while let Some(ddp_message) = reader.recv().await {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;36mserver\x1b[0m {ddp_message:?}");
        sink.send(ddp_message.try_into()?).await?;
    }

    Ok(())
}

async fn start_producer_client(
    mut stream: SplitStream<WebSocketStream<TcpStream>>,
    session: Arc<Session>,
) -> Result<(), Error> {
    while let Some(raw_message) = stream.try_next().await? {
        let ddp_message = DDPMessage::try_from(&raw_message)
            .with_context(|| format!("Invalid DDP message from client: {raw_message:?}"))?;
        println!("\x1b[0;34mclient\x1b[0m -> \x1b[0;33mrouter\x1b[0m {ddp_message:?}");
        process_message_client(&session, ddp_message).await?;
    }

    Ok(())
}

async fn start_producer_server(
    mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    session: Arc<Session>,
) -> Result<(), Error> {
    while let Some(raw_message) = stream.try_next().await? {
        let ddp_message = DDPMessage::try_from(&raw_message)
            .with_context(|| format!("Invalid DDP message from server: {raw_message:?}"))?;
        println!("\x1b[0;36mserver\x1b[0m -> \x1b[0;33mrouter\x1b[0m {ddp_message:?}");
        process_message_server(&session, ddp_message).await?;
    }

    Ok(())
}

pub async fn start_session(
    id: usize,
    subscriptions: Arc<Mutex<Subscriptions>>,
    client: WebSocketStream<TcpStream>,
    server: WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), Error> {
    let mut tasks = JoinSet::new();

    // Setup websockets queues and message consumers.
    let (client_sink, client_stream) = client.split();
    let (server_sink, server_stream) = server.split();

    let (client_writer, client_reader) = channel::<DDPMessage>(64);
    let (server_writer, server_reader) = channel::<DDPMessage>(64);

    tasks.spawn(start_consumer_client(client_reader, client_sink));
    tasks.spawn(start_consumer_server(server_reader, server_sink));

    // Setup session.
    let session = Arc::new(Session {
        id,
        client_writer: client_writer.clone(),
        server_writer,
        inflights: Mutex::new(Inflights::default()),
        mergebox: Arc::new(Mutex::new(Mergebox::new(client_writer.clone()))),
        subscriptions,
    });

    // Setup message producers.
    tasks.spawn(start_producer_client(client_stream, session.clone()));
    tasks.spawn(start_producer_server(server_stream, session.clone()));

    // Stop when any task's finished. Before the error is unwrapped (all of the
    // tasks will stop only when an error happens), stop all subscriptions made
    // in this session. (It's safe to `unwrap` here - there's always a task.)
    let result = tasks.join_next().await.unwrap();
    session
        .subscriptions
        .lock()
        .await
        .stop_all(session.id, &session.mergebox)
        .await?;
    result??;
    Ok(())
}
