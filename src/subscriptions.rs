use crate::cursor::{Cursor, CursorDescription};
use crate::inflights::Inflight;
use crate::mergebox::Mergebox;
use crate::watcher::Watcher;
use anyhow::{anyhow, Context, Error};
use mongodb::Database;
use serde::Deserialize;
use serde_json::{from_str, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

pub struct Subscriptions {
    cursors_by_collection: BTreeMap<String, Vec<Weak<Mutex<Cursor>>>>,
    #[allow(clippy::type_complexity)]
    cursors_by_session: BTreeMap<usize, BTreeMap<String, Vec<Arc<Mutex<Cursor>>>>>,
    database: Database,
    #[allow(clippy::struct_field_names)]
    server_subscriptions: BTreeSet<String>,
    watcher: Arc<Mutex<Watcher>>,
}

impl Subscriptions {
    pub fn is_server_subscription(&self, subscription: &str) -> bool {
        self.server_subscriptions.contains(subscription)
    }

    pub fn new(database: Database, watcher: Watcher) -> Self {
        Self {
            cursors_by_collection: BTreeMap::default(),
            cursors_by_session: BTreeMap::default(),
            database,
            server_subscriptions: BTreeSet::default(),
            watcher: Arc::new(Mutex::new(watcher)),
        }
    }

    pub async fn start(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
        inflight: &Inflight,
        subscription_id: &str,
        error: &Option<Value>,
        result: &Option<Value>,
    ) -> Result<(), Error> {
        // Check for errors.
        if let Some(error) = error {
            return Err(match error.get("reason") {
                Some(Value::String(message))
                    if Some(message)
                        .and_then(|x| x.strip_prefix("Method '__subscription__"))
                        .and_then(|x| x.strip_suffix("' not found"))
                        .is_some_and(|x| x == inflight.name) =>
                {
                    self.server_subscriptions.insert(inflight.name.clone());
                    anyhow!("Publication for {} was not registered", inflight.name)
                }
                _ => anyhow!(error.clone()),
            });
        }

        // Parse.
        let Some(Value::String(descriptions)) = result else {
            return Err(anyhow!("Incorrect format"));
        };

        let Value::Array(descriptions) = from_str(descriptions)? else {
            return Err(anyhow!("Incorrect format"));
        };

        let descriptions = descriptions
            .iter()
            .map(CursorDescription::deserialize)
            .collect::<Result<Vec<_>, _>>()?;

        // Start.
        let mut cursors = vec![];
        for description in descriptions {
            cursors.push(self.start_cursor(session_id, mergebox, description).await?);
        }

        self.cursors_by_session
            .entry(session_id)
            .or_default()
            .insert(subscription_id.to_owned(), cursors);
        Ok(())
    }

    async fn start_cursor(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
        description: CursorDescription,
    ) -> Result<Arc<Mutex<Cursor>>, Error> {
        // Search for existing cursor with the same description. While at it,
        // remove all empty references.
        let cursors = self
            .cursors_by_collection
            .entry(description.collection.clone())
            .or_default();
        for index in (0..cursors.len()).rev() {
            if let Some(cursor) = cursors[index].upgrade() {
                let is_deduplicated = {
                    let mut cursor = cursor.lock().await;
                    if *cursor.description() == description {
                        cursor.start(session_id, mergebox).await?;
                        true
                    } else {
                        false
                    }
                };

                if is_deduplicated {
                    return Ok(cursor);
                }
            } else {
                cursors.swap_remove(index);
            }
        }

        // Create and start a new cursor.
        let mut cursor = Cursor::new(self.database.clone(), description, self.watcher.clone());
        cursor.start(session_id, mergebox).await?;

        // Store a weak reference for faster lookups.
        let cursor = Arc::new(Mutex::new(cursor));
        cursors.push(Arc::downgrade(&cursor));
        Ok(cursor)
    }

    pub async fn stop(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
        subscription_id: &str,
    ) -> Result<Option<String>, Error> {
        if let Some((subscription_id, cursors)) = self
            .cursors_by_session
            .get_mut(&session_id)
            .and_then(|cursors| cursors.remove_entry(subscription_id))
        {
            for cursor in cursors {
                cursor
                    .lock()
                    .await
                    .stop(session_id, mergebox)
                    .await
                    .context("Subscriptions::stop")?;
            }
            Ok(Some(subscription_id))
        } else {
            Ok(None)
        }
    }

    pub async fn stop_all(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        if let Some(cursors) = self.cursors_by_session.remove(&session_id) {
            for cursor in cursors.into_values().flatten() {
                cursor
                    .lock()
                    .await
                    .stop(session_id, mergebox)
                    .await
                    .context("Subscriptions::stop_all")?;
            }
        }

        Ok(())
    }
}
