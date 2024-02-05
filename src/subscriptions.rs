use crate::cursor::{Cursor, CursorDescription, CursorFetcher};
use crate::inflights::Inflight;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, Error};
use mongodb::Database;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

pub struct Subscriptions {
    database: Database,
    mergebox: Arc<Mutex<Mergebox>>,
    cursors_by_collection: BTreeMap<String, Vec<Weak<Cursor>>>,
    cursors_by_subscription: BTreeMap<String, Vec<Arc<Cursor>>>,
}

impl Subscriptions {
    pub fn new(database: Database, mergebox: Arc<Mutex<Mergebox>>) -> Self {
        Self {
            database,
            mergebox,
            cursors_by_collection: BTreeMap::default(),
            cursors_by_subscription: BTreeMap::default(),
        }
    }

    pub async fn start(
        &mut self,
        inflight: &Inflight,
        id: String,
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
                    anyhow!("Publication for {} was not registered", inflight.name)
                }
                _ => anyhow!(error.clone()),
            });
        }

        // Parse.
        let Some(Value::Array(descriptions)) = result else {
            return Err(anyhow!("Incorrect format"));
        };

        let descriptions = descriptions
            .iter()
            .map(CursorDescription::deserialize)
            .collect::<Result<Vec<_>, _>>()?;

        // Start.
        let mut cursors = vec![];
        for description in descriptions {
            cursors.push(self.start_cursor(description).await?);
        }

        self.cursors_by_subscription.insert(id, cursors);
        Ok(())
    }

    async fn start_cursor(&mut self, description: CursorDescription) -> Result<Arc<Cursor>, Error> {
        // Search for existing cursor with the same description. While at it,
        // remove all empty references.
        let cursors = self
            .cursors_by_collection
            .entry(description.collection.clone())
            .or_default();
        for index in (0..cursors.len()).rev() {
            if let Some(existing) = cursors[index].upgrade() {
                if *existing.description() == description {
                    return Ok(existing);
                }
            } else {
                cursors.swap_remove(index);
            }
        }

        // Create and start a new cursor.
        let fetcher = CursorFetcher::new(self.database.clone(), description);
        let mut cursor = Cursor::from(fetcher);
        cursor.start(&self.mergebox).await?;

        // Store a weak reference for faster lookups.
        let cursor = Arc::new(cursor);
        cursors.push(Arc::downgrade(&cursor));
        Ok(cursor)
    }

    pub async fn stop(&mut self, id: &str) -> Result<Option<String>, Error> {
        if let Some((id, cursors)) = self.cursors_by_subscription.remove_entry(id) {
            for cursor in cursors {
                if let Some(cursor) = Arc::into_inner(cursor) {
                    cursor.stop(&self.mergebox).await?;
                }
            }
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}
