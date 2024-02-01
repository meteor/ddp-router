use crate::ddp::DDPMessage;
use crate::mergebox::Mergebox;
use crate::subscription::Subscription;
use anyhow::{anyhow, Result};
use bson::{to_document, Document};
use futures_util::{SinkExt, StreamExt};
use mongodb::Database;
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
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
    server_documents: BTreeMap<String, Vec<(Value, Document)>>,
}

impl Session {
    async fn flush_mergebox(&mut self) -> Result<()> {
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
            server_documents: BTreeMap::default(),
        }
    }

    async fn process_client(&mut self, mut ddp_message: DDPMessage) -> Result<()> {
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
                subscription.stop(&mut self.mergebox)?;
                return self.flush_mergebox().await;
            }
        }

        self.send_server(ddp_message).await
    }

    async fn process_server(&mut self, mut ddp_message: DDPMessage) -> Result<()> {
        // Hide router method calls.
        if let DDPMessage::Updated { methods } = &mut ddp_message {
            methods.retain(|id| !self.inflights.process_update(id));
            if methods.is_empty() {
                return Ok(());
            }
        }

        if let DDPMessage::Result { id, error, result } = &mut ddp_message {
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
                        Subscription::try_from((&*id, cursor_descriptions))
                    }
                    (None, _) => Err(anyhow!("Incorrect format")),
                };

                match parsed_subscription {
                    Ok(subscription) => {
                        // If the method succeeded and returned only supported
                        // cursor descriptions, register them as router-managed
                        // subscription.
                        self.subscriptions.insert(id.clone(), subscription);
                        return Ok(());
                    }
                    Err(error) => {
                        // If the method failed, did not provide a response,
                        // used an incorrect format, or requires an unsupported
                        // query, start a classic subscription instead.
                        println!("\x1b[0;31m{error}\x1b[0m");
                        self.send_server(DDPMessage::Sub {
                            id: id.clone(),
                            name: inflight.name,
                            params: inflight.params,
                        })
                        .await?;
                        return Ok(());
                    }
                }
            }
        }

        // Track server subscriptions in mergebox.
        if let DDPMessage::Added {
            id,
            collection,
            fields,
        } = &ddp_message
        {
            let document = fields
                .as_ref()
                .map_or_else(|| Ok(Document::new()), to_document)?;
            self.mergebox
                .insert(collection, id.clone(), document.clone())?;
            self.server_documents
                .entry(collection.clone())
                .or_default()
                .push((id.clone(), document));
            return self.flush_mergebox().await;
        }

        if let DDPMessage::Changed {
            id,
            collection,
            fields,
            cleared,
        } = &ddp_message
        {
            let documents = self
                .server_documents
                .get_mut(collection)
                .ok_or_else(|| anyhow!("Collection not found {collection}"))?;
            let index = documents
                .iter()
                .position(|x| x.0 == *id)
                .ok_or_else(|| anyhow!("Document not found {id}"))?;
            let (id, document) = documents.swap_remove(index);
            let mut document_applied = document.clone();
            for field in cleared.iter().flatten() {
                document_applied.remove(field);
            }
            if let Some(fields) = fields {
                for (key, value) in to_document(fields)? {
                    document_applied.insert(key, value);
                }
            }
            self.mergebox
                .insert(collection, id.clone(), document_applied.clone())?;
            self.mergebox.remove(collection, id.clone(), &document)?;
            documents.push((id, document_applied));
            return self.flush_mergebox().await;
        }

        if let DDPMessage::Removed { id, collection } = &ddp_message {
            let documents = self
                .server_documents
                .get_mut(collection)
                .ok_or_else(|| anyhow!("Collection not found {collection}"))?;
            let index = documents
                .iter()
                .position(|x| x.0 == *id)
                .ok_or_else(|| anyhow!("Document not found {id}"))?;
            let (id, document) = documents.swap_remove(index);
            self.mergebox.remove(collection, id.clone(), &document)?;
            return self.flush_mergebox().await;
        }

        self.send_client(ddp_message).await
    }

    async fn run_subscriptions(&mut self) -> Result<()> {
        for subscription in self.subscriptions.values_mut() {
            subscription
                .fetch(&mut self.database, &mut self.mergebox)
                .await?;
        }

        self.flush_mergebox().await
    }

    async fn send_client(&mut self, ddp_message: DDPMessage) -> Result<()> {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;34mclient\x1b[0m {ddp_message:?}");
        Ok(self.client.send(ddp_message.try_into()?).await?)
    }

    async fn send_server(&mut self, ddp_message: DDPMessage) -> Result<()> {
        println!("\x1b[0;33mrouter\x1b[0m -> \x1b[0;36mserver\x1b[0m {ddp_message:?}");
        Ok(self.server.send(ddp_message.try_into()?).await?)
    }

    pub async fn start(&mut self) -> Result<()> {
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
                    self.run_subscriptions().await?;
                },
            }
        }
    }
}
