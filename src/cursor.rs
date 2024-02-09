use crate::drop_handle::DropHandle;
use crate::ejson::into_ejson_document;
use crate::matcher::{is_matching, is_supported};
use crate::mergebox::{Mergebox, Mergeboxes};
use anyhow::{anyhow, Error};
use bson::{doc, Document};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::change_stream::ChangeStream;
use mongodb::options::{ChangeStreamOptions, FindOptions, FullDocumentType};
use mongodb::Database;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::mem::{replace, take};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::{interval_at, Duration, Instant};

const OK: Result<(), Error> = Ok(());

pub struct Cursor {
    description: CursorDescription,
    mergeboxes: Arc<Mutex<Mergeboxes>>,
    fetcher: Arc<Mutex<CursorFetcher>>,
    task: Option<DropHandle<Result<(), Error>>>,
}

impl Cursor {
    pub fn description(&self) -> &CursorDescription {
        &self.description
    }

    pub async fn start(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // Register new mergebox. If it is the first one, start the background
        // task. If not, add all already fetched documents to it.
        let is_first = {
            self.mergeboxes
                .lock()
                .await
                .insert_mergebox(session_id, mergebox)
        };
        if is_first {
            println!("\x1b[0;33mrouter\x1b[0m start({:?})", self.description);

            // Run initial query.
            let mergeboxes = self.mergeboxes.clone();
            self.fetcher.lock().await.fetch(&mergeboxes).await?;

            // Start background task.
            let fetcher = self.fetcher.clone();
            let task = async move {
                // Separate context to eliminate locking.
                let maybe_change_stream = { fetcher.lock().await.create_change_stream().await? };

                // Start a Change Stream or fall back to pooling.
                if let Some(mut change_stream) = maybe_change_stream {
                    while let Some(event) = change_stream.try_next().await? {
                        fetcher
                            .lock()
                            .await
                            .handle_change_stream_event(event, &mergeboxes)
                            .await?;
                    }

                    OK
                } else {
                    // TODO: Make interval configurable.
                    let interval = Duration::from_secs(5);
                    let mut timer = interval_at(Instant::now() + interval, interval);
                    loop {
                        timer.tick().await;
                        fetcher.lock().await.fetch(&mergeboxes).await?;
                    }
                }
            }
            // TODO: Better handling of subtasks.
            .then(|result| async move {
                if let Err(error) = &result {
                    println!("\x1b[0;31m[[ERROR]] {error}\x1b[0m");
                }
                result
            });
            let _ = self.task.insert(DropHandle::new(spawn(task)));
        } else {
            let documents = { self.fetcher.lock().await.documents.clone() };

            let mut mergebox = mergebox.lock().await;
            for mut document in documents {
                let id = extract_id(&mut document)?;
                mergebox
                    .insert(self.description.collection.clone(), id, document)
                    .await?;
            }
        }

        OK
    }

    pub async fn stop(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // Unregister all documents.
        {
            let documents = { self.fetcher.lock().await.documents.clone() };
            let mut mergebox = mergebox.lock().await;
            for mut document in documents {
                let id = extract_id(&mut document)?;
                mergebox
                    .remove(self.description.collection.clone(), id, &document)
                    .await?;
            }
        }

        // If it is the last one, stop the cursor. If it is the last one, stop
        // the background task.
        let is_last = { self.mergeboxes.lock().await.remove_mergebox(session_id) };
        if is_last {
            println!("\x1b[0;33mrouter\x1b[0m  stop({:?})", self.description);

            // Shutdown task (if any).
            if let Some(task) = self.task.take() {
                task.shutdown().await;
            }
        }

        OK
    }
}

impl From<CursorFetcher> for Cursor {
    fn from(fetcher: CursorFetcher) -> Self {
        Self {
            description: fetcher.description.clone(),
            mergeboxes: Arc::new(Mutex::new(Mergeboxes::default())),
            fetcher: Arc::new(Mutex::new(fetcher)),
            task: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CursorDescription {
    #[serde(rename = "collectionName")]
    pub collection: String,
    pub selector: Document,
    pub options: CursorOptions,
}

pub struct CursorFetcher {
    database: Database,
    description: CursorDescription,
    documents: Vec<Map<String, Value>>,
}

impl CursorFetcher {
    async fn create_change_stream(
        &self,
    ) -> Result<Option<ChangeStream<ChangeStreamEvent<Document>>>, Error> {
        let CursorDescription {
            collection,
            selector,
            options,
        } = &self.description;

        // We have to understand the selector to process the Change Stream
        // events correctly.
        if !is_supported(selector) {
            println!(
                "\x1b[0;32mmongo\x1b[0m \x1b[0;31mselector not supported\x1b[0m ({selector:?})"
            );
            return Ok(None);
        }

        // TODO: Implement MongoDB projections.
        let has_projection = options
            .projection
            .as_ref()
            .map_or(false, |projection| !projection.is_empty());
        if has_projection {
            println!(
                "\x1b[0;32mmongo\x1b[0m \x1b[0;31mprojection not supported\x1b[0m ({:?})",
                options.projection
            );
            return Ok(None);
        }

        // TODO: Implement MongoDB sorting.
        if options.limit.is_some() || options.skip.is_some() || options.sort.is_some() {
            println!("\x1b[0;32mmongo\x1b[0m \x1b[0;31moptions not supported\x1b[0m ({options:?})");
            return Ok(None);
        }

        // TODO: Reusue Change Streams between `Cursor`s.
        // The current Meteor's Oplog tailing has to refetch a document by `_id`
        // when a document outside of the current documents set is updated and
        // it _may_ match the selector now. With Change Streams we can skip that
        // by fetching the full documents.
        // https://github.com/meteor/meteor/blob/7411b3c85a3c95a6b6f3c588babe6eae894d6fb6/packages/mongo/oplog_observe_driver.js#L652
        let pipeline = [
            doc! { "$match": { "operationType": { "$in": ["delete", "drop", "dropDatabase", "insert", "update"] } } },
            doc! { "$project": { "_id": 1, "documentKey": 1, "fullDocument": 1, "ns": 1, "operationType": 1 } },
        ];
        let options = ChangeStreamOptions::builder()
            // TODO: Ideally we would use `Required` here, but it has to be
            // enabled on the database level. It should be configurable.
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();
        let change_stream = self
            .database
            .collection(collection)
            .watch(pipeline, Some(options))
            .await?;
        Ok(Some(change_stream))
    }

    async fn handle_change_stream_event(
        &mut self,
        event: ChangeStreamEvent<Document>,
        mergeboxes: &Arc<Mutex<Mergeboxes>>,
    ) -> Result<(), Error> {
        match event {
            ChangeStreamEvent {
                operation_type: OperationType::Delete,
                document_key: Some(document),
                ..
            } => {
                let mut document = into_ejson_document(document);
                let id = extract_id(&mut document)?;
                let index = self
                    .documents
                    .iter()
                    .position(|x| x.get("_id") == Some(&id))
                    .ok_or_else(|| anyhow!("Document {id} not found"))?;
                let mut document = self.documents.swap_remove(index);
                document.remove("_id");
                mergeboxes
                    .lock()
                    .await
                    .remove(self.description.collection.clone(), id, &document)
                    .await
            }
            ChangeStreamEvent {
                operation_type: OperationType::Drop | OperationType::DropDatabase,
                ..
            } => {
                let mut mergeboxes = mergeboxes.lock().await;
                for mut document in take(&mut self.documents) {
                    let id = extract_id(&mut document)?;
                    mergeboxes
                        .remove(self.description.collection.clone(), id, &document)
                        .await?;
                }
                OK
            }
            ChangeStreamEvent {
                operation_type: OperationType::Insert,
                full_document: Some(document),
                ..
            } => {
                let mut document = into_ejson_document(document);
                if !is_matching(&self.description.selector, &document) {
                    return OK;
                }

                self.documents.push(document.clone());
                let id = extract_id(&mut document)?;
                mergeboxes
                    .lock()
                    .await
                    .insert(self.description.collection.clone(), id, document)
                    .await
            }
            ChangeStreamEvent {
                operation_type: OperationType::Update,
                full_document: Some(document),
                ..
            } => {
                let mut document = into_ejson_document(document);
                let is_matching = is_matching(&self.description.selector, &document);
                if is_matching {
                    self.documents.push(document.clone());
                }

                let id = extract_id(&mut document)?;
                if is_matching {
                    mergeboxes
                        .lock()
                        .await
                        .insert(self.description.collection.clone(), id.clone(), document)
                        .await?;
                }

                let maybe_index = self
                    .documents
                    .iter()
                    .position(|x| x.get("_id") == Some(&id));
                if let Some(index) = maybe_index {
                    let mut document = self.documents.swap_remove(index);
                    document.remove("_id");
                    mergeboxes
                        .lock()
                        .await
                        .remove(self.description.collection.clone(), id, &document)
                        .await?;
                }

                OK
            }
            // TODO: Handle other events.
            _ => Err(anyhow!("Unsupported event {event:?}")),
        }
    }

    async fn fetch(&mut self, mergeboxes: &Arc<Mutex<Mergeboxes>>) -> Result<(), Error> {
        println!("\x1b[0;32mmongo\x1b[0m  fetch({:?})", self.description);

        let mut documents: Vec<_> = self
            .database
            .collection::<Document>(&self.description.collection)
            .find(
                Some(self.description.selector.clone()),
                Some(self.description.options.clone().into()),
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

        OK
    }

    pub fn new(database: Database, description: CursorDescription) -> Self {
        Self {
            database,
            description,
            documents: Vec::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CursorOptions {
    pub limit: Option<i64>,
    pub projection: Option<Document>,
    pub skip: Option<u64>,
    pub sort: Option<Document>,
    pub transform: Option<()>,
}

impl From<CursorOptions> for FindOptions {
    fn from(options: CursorOptions) -> Self {
        Self::builder()
            .limit(options.limit)
            .projection(options.projection)
            .skip(options.skip)
            .sort(options.sort)
            .build()
    }
}

fn extract_id(document: &mut Map<String, Value>) -> Result<Value, Error> {
    document
        .remove("_id")
        .ok_or_else(|| anyhow!("_id not found in {document:?}"))
}
