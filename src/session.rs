use crate::ddp::DDPMessage;
use crate::mergebox::Mergebox;
use crate::subscription::Subscription;
use anyhow::{anyhow, Error};
use futures_util::{SinkExt, StreamExt};
use mongodb::Database;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

struct Inflight {
    name: String,
    params: Option<Vec<Value>>,
    update_received: bool,
}

impl Inflight {
    fn new(name: String, params: Option<Vec<Value>>) -> Self {
        Self {
            name,
            params,
            update_received: false,
        }
    }
}

#[derive(Default)]
struct Inflights(BTreeMap<String, Option<Inflight>>);

impl Inflights {
    fn process_result(&mut self, id: &String) -> Option<Inflight> {
        let update_received = self
            .0
            .get(id)?
            .as_ref()
            .is_some_and(|inflight| inflight.update_received);
        if update_received {
            self.0.remove(id)?
        } else {
            self.0.get_mut(id)?.take()
        }
    }

    fn process_update(&mut self, id: &String) -> bool {
        if let Some(maybe_inflight) = self.0.get_mut(id) {
            if let Some(inflight) = maybe_inflight {
                inflight.update_received = true;
            } else {
                self.0.remove(id);
            }
            true
        } else {
            false
        }
    }

    fn register(&mut self, id: String, inflight: Inflight) {
        self.0.insert(id, Some(inflight));
    }
}

pub struct Session {
    // MongoDB
    database: Database,

    // Streams
    client: WebSocketStream<TcpStream>,
    server: WebSocketStream<MaybeTlsStream<TcpStream>>,

    // Internals
    inflights: Inflights,
    mergebox: Mergebox,
    subscriptions: BTreeMap<String, Subscription>,
}

impl Session {
    async fn flush_mergebox(&mut self) -> Result<(), Error> {
        for message in self.mergebox.flush() {
            self.send_client(message).await?;
        }

        Ok(())
    }

    pub fn new(
        database: Database,
        client: WebSocketStream<TcpStream>,
        server: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Self {
        Self {
            database,
            client,
            server,
            inflights: Inflights::default(),
            mergebox: Mergebox::default(),
            subscriptions: BTreeMap::default(),
        }
    }

    async fn pool_subscriptions(&mut self) -> Result<(), Error> {
        for subscription in self.subscriptions.values_mut() {
            subscription
                .pool(&mut self.database, &mut self.mergebox)
                .await?;
        }

        self.flush_mergebox().await
    }

    async fn process_client(&mut self, mut ddp_message: DDPMessage) -> Result<(), Error> {
        // Intercept a client subscription into a router-managed subscription.
        if let DDPMessage::Sub { id, name, params } = ddp_message {
            ddp_message = DDPMessage::Method {
                id: id.clone(),
                method: format!("__subscription__{name}"),
                params: params.clone(),
                random_seed: None,
            };
            self.inflights.register(id, Inflight::new(name, params));
        }

        // Intercept a client unsubscription of a router-managed subscription.
        if let DDPMessage::Unsub { id } = &ddp_message {
            if let Some(subscription) = self.subscriptions.remove(id) {
                subscription.stop(&mut self.mergebox).await?;
                return self.flush_mergebox().await;
            }
        }

        self.send_server(ddp_message).await
    }

    async fn process_server(&mut self, ddp_message: DDPMessage) -> Result<(), Error> {
        match ddp_message {
            // Hide router method calls.
            DDPMessage::Result {
                ref id,
                ref error,
                ref result,
            } => {
                if let Some(inflight) = self.inflights.process_result(id) {
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
                            Subscription::try_from((id, cursor_descriptions))
                        }
                        (None, _) => Err(anyhow!("Incorrect format")),
                    };

                    match parsed_subscription {
                        Ok(mut subscription) => {
                            // If the method succeeded and returned only
                            // supported cursor descriptions, register them as
                            // router-managed subscription.
                            subscription
                                .start(&mut self.database, &mut self.mergebox)
                                .await?;
                            self.flush_mergebox().await?;
                            self.subscriptions.insert(id.clone(), subscription);
                        }
                        Err(error) => {
                            // If the method failed, did not provide a response,
                            // used an incorrect format, or requires an
                            // unsupported query option, start a classic server
                            // subscription instead.
                            println!("\x1b[0;31m{error}\x1b[0m");
                            self.send_server(DDPMessage::Sub {
                                id: id.clone(),
                                name: inflight.name,
                                params: inflight.params,
                            })
                            .await?;
                        }
                    }

                    return Ok(());
                }

                self.send_client(ddp_message).await
            }

            DDPMessage::Updated { mut methods } => {
                methods.retain(|id| !self.inflights.process_update(id));
                if methods.is_empty() {
                    return Ok(());
                }

                self.send_client(DDPMessage::Updated { methods }).await
            }

            // Track server subscriptions in mergebox.
            DDPMessage::Added {
                id,
                collection,
                fields,
            } => {
                self.mergebox.server_added(collection, id, fields)?;
                self.flush_mergebox().await
            }

            DDPMessage::Changed {
                id,
                collection,
                fields,
                cleared,
            } => {
                self.mergebox
                    .server_changed(collection, id, fields, cleared)?;
                self.flush_mergebox().await
            }

            DDPMessage::Removed { id, collection } => {
                self.mergebox.server_removed(collection, id)?;
                self.flush_mergebox().await
            }

            // Pass-through other DDP messages.
            _ => self.send_client(ddp_message).await,
        }
    }

    async fn send_client(&mut self, ddp_message: DDPMessage) -> Result<(), Error> {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;34mclient\x1b[0m {ddp_message:?}");
        Ok(self.client.send(ddp_message.try_into()?).await?)
    }

    async fn send_server(&mut self, ddp_message: DDPMessage) -> Result<(), Error> {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;36mserver\x1b[0m {ddp_message:?}");
        Ok(self.server.send(ddp_message.try_into()?).await?)
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut interval = interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                Some(raw_message) = self.client.next() => {
                    let ddp_message = DDPMessage::try_from(raw_message?)?;
                    println!("\x1b[0;34mclient\x1b[0m -> \x1b[0;33mrouter\x1b[0m {ddp_message:?}");
                    self.process_client(ddp_message).await?;
                },
                Some(raw_message) = self.server.next() => {
                    let ddp_message = DDPMessage::try_from(raw_message?)?;
                    println!("\x1b[0;36mserver\x1b[0m -> \x1b[0;33mrouter\x1b[0m {ddp_message:?}");
                    self.process_server(ddp_message).await?;
                },
                _ = interval.tick() => {
                    self.pool_subscriptions().await?;
                },
            }
        }
    }
}
