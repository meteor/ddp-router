use crate::drop_handle::DropHandle;
use crate::ejson::into_ejson_document;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, Error};
use bson::{doc, Document};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::change_stream::ChangeStream;
use mongodb::options::FindOptions;
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
    fetcher: Arc<Mutex<CursorFetcher>>,
    task: Option<DropHandle<Result<(), Error>>>,
}

impl Cursor {
    pub fn description(&self) -> &CursorDescription {
        &self.description
    }

    pub async fn start(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        println!("\x1b[0;33mrouter\x1b[0m start({:?})", self.description);

        // Run initial query.
        self.fetcher.lock().await.fetch(mergebox).await?;

        // Start background task.
        let mergebox = mergebox.clone();
        let fetcher = self.fetcher.clone();
        let _ = self.task.insert(DropHandle::new(spawn(async move {
            // Separate context to eliminate locking.
            let maybe_change_stream = { fetcher.lock().await.create_change_stream().await? };

            // Start a Change Stream or fall back to pooling.
            if let Some(mut change_stream) = maybe_change_stream {
                while let Some(event) = change_stream.try_next().await? {
                    fetcher
                        .lock()
                        .await
                        .handle_change_stream_event(event, &mergebox)
                        .await?;
                }

                OK
            } else {
                // TODO: Make interval configurable.
                let interval = Duration::from_secs(5);
                let mut timer = interval_at(Instant::now() + interval, interval);
                loop {
                    timer.tick().await;
                    fetcher.lock().await.fetch(&mergebox).await?;
                }
            }
        })));

        OK
    }

    pub async fn stop(mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        println!("\x1b[0;33mrouter\x1b[0m  stop({:?})", self.description);

        // Shutdown task (if any).
        if let Some(task) = self.task.take() {
            task.shutdown().await;
        }

        // Unregister all documents.
        let documents = Arc::into_inner(self.fetcher)
            .ok_or_else(|| anyhow!("Failed to consume Query"))?
            .into_inner()
            .documents;

        let mut mergebox = mergebox.lock().await;
        for mut document in documents {
            let id = extract_id(&mut document)?;
            mergebox
                .remove(self.description.collection.clone(), id, &document)
                .await?;
        }

        OK
    }
}

impl From<CursorFetcher> for Cursor {
    fn from(fetcher: CursorFetcher) -> Self {
        Self {
            description: fetcher.description.clone(),
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
        // FIXME: Change Streams DO NOT work at the moment.
        if true {
            return Ok(None);
        }

        let CursorDescription {
            collection,
            selector,
            options,
        } = &self.description;
        if options.limit.is_some() && options.skip.is_some() {
            return Ok(None);
        }

        let mut pipeline = vec![doc! { "$match": { "$expr": selector.clone() } }];
        if let Some(projection) = &options.projection {
            pipeline.push(doc! { "$project": projection.clone() });
        }

        let change_stream = self
            .database
            .collection::<Document>(collection)
            .watch(pipeline, None)
            .await?;
        Ok(Some(change_stream))
    }

    async fn handle_change_stream_event(
        &mut self,
        event: ChangeStreamEvent<Document>,
        mergebox: &Arc<Mutex<Mergebox>>,
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
                mergebox
                    .lock()
                    .await
                    .remove(self.description.collection.clone(), id, &document)
                    .await
            }
            ChangeStreamEvent {
                operation_type: OperationType::Drop | OperationType::DropDatabase,
                ..
            } => {
                let mut mergebox = mergebox.lock().await;
                for mut document in take(&mut self.documents) {
                    let id = extract_id(&mut document)?;
                    mergebox
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
                self.documents.push(document.clone());
                let id = extract_id(&mut document)?;
                mergebox
                    .lock()
                    .await
                    .insert(self.description.collection.clone(), id, document)
                    .await
            }
            // TODO: Handle other events.
            _ => todo!("{event:?}"),
        }
    }

    async fn fetch(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
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

        let mut mergebox = mergebox.lock().await;

        for document in &mut documents {
            let id = extract_id(document)?;
            mergebox
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
            mergebox
                .remove(self.description.collection.clone(), id, &document)
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
