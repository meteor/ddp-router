use super::description::CursorDescription;
use super::viewer::CursorViewer;
use crate::ejson::into_ejson_document;
use crate::mergebox::{Mergebox, Mergeboxes};
use crate::watcher::{Event, Watcher};
use anyhow::anyhow;
use anyhow::Error;
use bson::Document;
use futures_util::{StreamExt, TryStreamExt};
use mongodb::Database;
use serde_json::{Map, Value};
use std::mem::{replace, take};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::time::{interval_at, Duration, Instant, Interval};

pub struct CursorFetcher {
    database: Database,
    description: CursorDescription,
    documents: Vec<Map<String, Value>>,
    viewer: Option<CursorViewer>,
    watcher: Arc<Mutex<Watcher>>,
}

impl CursorFetcher {
    pub async fn fetch(&mut self, mergeboxes: &Arc<Mutex<Mergeboxes>>) -> Result<(), Error> {
        println!("\x1b[0;32mmongo\x1b[0m fetch({:?})", self.description);

        let mut documents: Vec<_> = self
            .database
            .collection::<Document>(&self.description.collection)
            .find(
                Some(self.description.selector.clone()),
                Some(self.description.as_find_options()),
            )
            .await?
            .map(|maybe_document| maybe_document.map(into_ejson_document))
            .try_collect()
            .await?;

        let mut mergeboxes = mergeboxes.lock().await;

        for document in &mut documents {
            let id = extract_id(document)?;
            mergeboxes
                .insert(
                    self.description.collection.clone(),
                    id.clone(),
                    document.clone(),
                )
                .await?;
            document.insert(String::from("_id"), id);
        }

        for mut document in replace(&mut self.documents, documents) {
            let id = extract_id(&mut document)?;
            mergeboxes
                .remove(self.description.collection.clone(), id.clone(), &document)
                .await?;
        }

        Ok(())
    }

    pub fn new(
        database: Database,
        description: CursorDescription,
        watcher: Arc<Mutex<Watcher>>,
    ) -> Self {
        let viewer = match CursorViewer::try_from(&description) {
            Ok(viewer) => Some(viewer),
            Err(error) => {
                println!("\x1b[0;32mmongo\x1b[0m \x1b[0;31m{error}\x1b[0m");
                None
            }
        };

        Self {
            database,
            description,
            documents: Vec::default(),
            viewer,
            watcher,
        }
    }

    pub async fn process(
        &mut self,
        event: Event,
        mergeboxes: &Arc<Mutex<Mergeboxes>>,
    ) -> Result<(), Error> {
        let refetch = process(
            event,
            &self.description,
            &mut self.documents,
            mergeboxes,
            self.viewer.as_ref().unwrap(),
        )
        .await?;
        if refetch {
            self.fetch(mergeboxes).await?;
        }

        Ok(())
    }

    pub async fn register(&self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        let mut mergebox = mergebox.lock().await;
        for mut document in self.documents.clone() {
            let id = extract_id(&mut document)?;
            mergebox
                .insert(self.description.collection.clone(), id, document)
                .await?;
        }

        Ok(())
    }

    pub async fn watch(&self) -> Result<Receiver<Event>, Interval> {
        if self.viewer.is_some() {
            let mut watcher = self.watcher.lock().await;
            Ok(watcher.watch(self.description.collection.clone()).await)
        } else {
            // Meteor's default.
            let interval = self.description.polling_interval_ms.unwrap_or(10_000);
            let duration = Duration::from_millis(interval);
            Err(interval_at(Instant::now() + duration, duration))
        }
    }

    pub async fn unregister(&self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        let mut mergebox = mergebox.lock().await;
        for mut document in self.documents.clone() {
            let id = extract_id(&mut document)?;
            mergebox
                .remove(self.description.collection.clone(), id, &document)
                .await?;
        }

        Ok(())
    }
}

fn extract_id(document: &mut Map<String, Value>) -> Result<Value, Error> {
    document
        .remove("_id")
        .ok_or_else(|| anyhow!("_id not found in {document:?}"))
}

async fn process(
    event: Event,
    description: &CursorDescription,
    documents: &mut Vec<Map<String, Value>>,
    mergeboxes: &Arc<Mutex<Mergeboxes>>,
    viewer: &CursorViewer,
) -> Result<bool, Error> {
    match event {
        Event::Clear => {
            let mut mergeboxes = mergeboxes.lock().await;
            for mut document in take(documents) {
                let id = extract_id(&mut document)?;
                viewer.projector.apply(&mut document);
                mergeboxes
                    .remove(description.collection.clone(), id, &document)
                    .await?;
            }
            Ok(false)
        }
        Event::Delete(document) => {
            let mut document = into_ejson_document(document);
            let id = extract_id(&mut document)?;
            let Some(index) = documents.iter().position(|x| x.get("_id") == Some(&id)) else {
                return Ok(false);
            };

            if description
                .limit()
                .is_some_and(|limit| limit == documents.len())
            {
                return Ok(true);
            }

            let mut document = documents.swap_remove(index);
            document.remove("_id");
            viewer.projector.apply(&mut document);
            mergeboxes
                .lock()
                .await
                .remove(description.collection.clone(), id, &document)
                .await?;

            Ok(false)
        }
        Event::Insert(document) => {
            let mut document = into_ejson_document(document);
            if !viewer.matcher.matches(&document) {
                return Ok(false);
            }

            if let Some(limit) = description.limit() {
                let index = documents
                    .binary_search_by(|x| viewer.sorter.cmp(x, &document))
                    .unwrap_or_else(|index| index);
                if index == limit {
                    return Ok(false);
                }

                documents.insert(index, document.clone());
            } else {
                documents.push(document.clone());
            }

            let id = extract_id(&mut document)?;
            viewer.projector.apply(&mut document);
            let mut mergeboxes = mergeboxes.lock().await;
            mergeboxes
                .insert(description.collection.clone(), id, document)
                .await?;

            if let Some(limit) = description.limit() {
                if documents.len() > limit {
                    if let Some(mut document) = documents.pop() {
                        let id = extract_id(&mut document)?;
                        viewer.projector.apply(&mut document);
                        mergeboxes
                            .remove(description.collection.clone(), id, &document)
                            .await?;
                    }
                }
            }

            Ok(false)
        }
        Event::Update(document) => {
            let mut document = into_ejson_document(document);
            let is_matching = viewer.matcher.matches(&document);
            if is_matching {
                let index_before = {
                    let id = document.get("_id");
                    documents.iter().position(|x| x.get("_id") == id)
                };

                if let Some(limit) = description.limit() {
                    let index = documents
                        .binary_search_by(|x| viewer.sorter.cmp(x, &document))
                        .unwrap_or_else(|index| index);

                    // Skip newly matching documents that don't fit in `limit`.
                    if index == limit {
                        return Ok(false);
                    }

                    documents.insert(index, document.clone());
                } else {
                    documents.push(document.clone());
                }

                let id = extract_id(&mut document)?;
                viewer.projector.apply(&mut document);
                let mut mergeboxes = mergeboxes.lock().await;
                mergeboxes
                    .insert(description.collection.clone(), id.clone(), document)
                    .await?;

                if let Some(index) = index_before {
                    let mut document = if description.limit().is_some() {
                        documents.remove(index)
                    } else {
                        documents.swap_remove(index)
                    };

                    document.remove("_id");
                    mergeboxes
                        .remove(description.collection.clone(), id, &document)
                        .await?;
                }
            } else {
                let id = extract_id(&mut document)?;
                let Some(index) = documents.iter().position(|x| x.get("_id") == Some(&id)) else {
                    return Ok(false);
                };

                let mut document = match description.limit() {
                    Some(limit) => {
                        // If we fall below the limit, we need to refetch.
                        if limit == documents.len() {
                            return Ok(true);
                        };

                        documents.remove(index)
                    }
                    None => documents.swap_remove(index),
                };

                let id = extract_id(&mut document)?;
                mergeboxes
                    .lock()
                    .await
                    .remove(description.collection.clone(), id, &document)
                    .await?;
            }

            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{process, CursorDescription, CursorViewer};
    use crate::ddp::DDPMessage;
    use crate::mergebox::{Mergebox, Mergeboxes};
    use crate::watcher::Event;
    use anyhow::Error;
    use bson::doc;
    use serde::Deserialize;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio::sync::mpsc::channel;
    use tokio::sync::Mutex;
    use tokio::test;

    async fn simulate(
        description: Value,
        events: Vec<Event>,
        messages: Vec<DDPMessage>,
    ) -> Result<(), Error> {
        let description = CursorDescription::deserialize(description)?;
        let viewer = CursorViewer::try_from(&description)?;
        let (sender, mut receiver) = channel(64);
        let mergeboxes = Arc::new(Mutex::new({
            let mut mergeboxes = Mergeboxes::default();
            mergeboxes.insert_mergebox(1, &Arc::new(Mutex::new(Mergebox::new(sender))));
            mergeboxes
        }));

        let mut documents = Vec::new();
        for event in events {
            process(event, &description, &mut documents, &mergeboxes, &viewer).await?;
        }

        for message in messages {
            assert_eq!(receiver.try_recv(), Ok(message));
        }

        assert!(receiver.try_recv().is_err());
        assert!(documents.is_empty());

        Ok(())
    }

    macro_rules! simulate {
        ($name:ident, $description:expr, $events:expr, $messages:expr) => {
            #[test]
            async fn $name() -> Result<(), Error> {
                simulate($description, $events, $messages).await
            }
        };
    }

    macro_rules! json_doc {
        ($($json:tt)*) => {
            match json! {{ $($json)* }} {
                Value::Object(map) => map,
                _ => unreachable!(),
            }
        };
    }

    simulate!(
        scenario_1,
        json! {{"collectionName": "x", "selector": {}, "options": {}}},
        vec![],
        vec![]
    );

    simulate!(
        scenario_2,
        json! {{"collectionName": "x", "selector": {}, "options": {}}},
        vec![
            Event::Insert(doc! {"_id": 1}),
            Event::Insert(doc! {"_id": 2, "a": 3}),
            Event::Clear
        ],
        vec![
            DDPMessage::Added {
                collection: "x".to_owned(),
                id: json!(1),
                fields: None
            },
            DDPMessage::Added {
                collection: "x".to_owned(),
                id: json!(2),
                fields: Some(json_doc! {"a": 3})
            },
            DDPMessage::Removed {
                collection: "x".to_owned(),
                id: json!(1)
            },
            DDPMessage::Removed {
                collection: "x".to_owned(),
                id: json!(2)
            }
        ]
    );
}
