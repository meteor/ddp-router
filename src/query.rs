use crate::drop_handle::DropHandle;
use crate::ejson::into_ejson_document;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, bail, Error};
use bson::{doc, to_document, Document};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::change_stream::ChangeStream;
use mongodb::options::FindOptions;
use mongodb::Database;
use serde_json::{Map, Value};
use std::mem::{replace, take};
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::interval;

const OK: Result<(), Error> = Ok(());

pub struct Query {
    query: Arc<Mutex<QueryInner>>,
    task: Option<DropHandle<Result<(), Error>>>,
}

impl Query {
    pub async fn is_same_query(&self, other: &Self) -> bool {
        *self.query.lock().await == *other.query.lock().await
    }

    pub async fn start(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        let mergebox = mergebox.clone();
        let query = self.query.clone();
        let _ = self.task.insert(DropHandle::new(spawn(async move {
            // Fetch initial results.
            let maybe_change_stream = {
                let mut query = query.lock().await;
                query.fetch(&mergebox).await?;
                query.create_change_stream().await?
            };

            // Start a Change Stream or fall back to pooling.
            if let Some(mut change_stream) = maybe_change_stream {
                while let Some(event) = change_stream.try_next().await? {
                    query
                        .lock()
                        .await
                        .handle_change_stream_event(event, &mergebox)
                        .await?;
                }

                OK
            } else {
                // TODO: Make interval configurable.
                let mut timer = interval(Duration::from_secs(5));
                loop {
                    timer.tick().await;
                    query.lock().await.fetch(&mergebox).await?;
                }
            }
        })));

        OK
    }

    pub async fn stop(mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        // Shutdown task (if any).
        if let Some(task) = self.task.take() {
            task.shutdown().await;
        }

        // Unregister all documents.
        let (collection, documents) = Arc::into_inner(self.query)
            .ok_or_else(|| anyhow!("Failed to consume Query"))?
            .into_inner()
            .take();

        let mut mergebox = mergebox.lock().await;
        for mut document in documents {
            let id = extract_id(&mut document)?;
            mergebox.remove(collection.clone(), id, &document).await?;
        }

        OK
    }
}

impl TryFrom<(&Database, &Value)> for Query {
    type Error = Error;
    fn try_from((database, value): (&Database, &Value)) -> Result<Self, Self::Error> {
        let Value::String(collection) = value
            .get("collectionName")
            .ok_or_else(|| anyhow!("Missing collectionName"))?
        else {
            bail!("Incorrect collectionName (expected a string)");
        };

        let selector = to_document(
            value
                .get("selector")
                .ok_or_else(|| anyhow!("Missing selector"))?,
        )?;

        let options_raw = value
            .get("options")
            .ok_or_else(|| anyhow!("Missing options"))?
            .clone();
        let options = to_options(&options_raw)?;

        let query = QueryInner {
            database: database.clone(),
            collection: collection.clone(),
            selector,
            options,
            options_raw,
            documents: Vec::default(),
        };

        Ok(Self {
            query: Arc::new(Mutex::new(query)),
            task: None,
        })
    }
}

struct QueryInner {
    database: Database,
    collection: String,
    selector: Document,
    options: FindOptions,
    options_raw: Value,
    documents: Vec<Map<String, Value>>,
}

impl QueryInner {
    async fn create_change_stream(
        &self,
    ) -> Result<Option<ChangeStream<ChangeStreamEvent<Document>>>, Error> {
        // FIXME: Change Streams DO NOT work at the moment.
        if false {
            return Ok(None);
        }

        if self.options.limit.is_some() && self.options.skip.is_some() {
            return Ok(None);
        }

        let mut pipeline = vec![doc! { "$match": { "$expr": self.selector.clone() } }];
        if let Some(projection) = &self.options.projection {
            pipeline.push(doc! { "$project": projection.clone() });
        }

        let change_stream = self
            .database
            .collection::<Document>(&self.collection)
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
                    .remove(self.collection.clone(), id, &document)
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
                        .remove(self.collection.clone(), id, &document)
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
                    .insert(self.collection.clone(), id, document)
                    .await
            }
            // TODO: Handle other events.
            _ => todo!("{event:?}"),
        }
    }

    async fn fetch(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        let mut documents: Vec<_> = self
            .database
            .collection::<Document>(&self.collection)
            .find(Some(self.selector.clone()), Some(self.options.clone()))
            .await?
            .map(|maybe_document| maybe_document.map(into_ejson_document))
            .try_collect()
            .await?;

        let mut mergebox = mergebox.lock().await;

        for document in &mut documents {
            let id = extract_id(document)?;
            mergebox
                .insert(self.collection.clone(), id.clone(), document.clone())
                .await?;
            document.insert(String::from("_id"), id);
        }

        for mut document in replace(&mut self.documents, documents) {
            let id = extract_id(&mut document)?;
            mergebox
                .remove(self.collection.clone(), id, &document)
                .await?;
        }

        OK
    }

    fn take(self) -> (String, Vec<Map<String, Value>>) {
        (self.collection, self.documents)
    }
}

impl PartialEq for QueryInner {
    fn eq(&self, other: &Self) -> bool {
        self.collection == other.collection
            && self.selector == other.selector
            && self.options_raw == other.options_raw
    }
}

fn extract_id(document: &mut Map<String, Value>) -> Result<Value, Error> {
    document
        .remove("_id")
        .ok_or_else(|| anyhow!("_id not found in {document:?}"))
}

fn to_options(options: &Value) -> Result<FindOptions, Error> {
    let Value::Object(options) = options else {
        bail!("Incorrect options (expected an object)");
    };

    options
        .into_iter()
        .try_fold(FindOptions::default(), |mut options, (option, value)| {
            match (option.as_str(), value) {
                ("limit", Value::Number(limit)) => match limit.as_i64() {
                    None => bail!("Invalid limit = {limit:?}"),
                    limit => options.limit = limit,
                },
                ("projection", Value::Object(projection)) => {
                    options.projection = Some(to_document(&projection)?);
                }
                ("skip", Value::Number(skip)) => match skip.as_u64() {
                    None => bail!("Invalid skip = {skip:?}"),
                    skip => options.skip = skip,
                },
                ("sort", Value::Object(sort)) => {
                    options.sort = Some(to_document(&sort)?);
                }
                ("transform", Value::Null) => {}
                (option, value) => bail!("Unknown option {option} = {value:?}"),
            }

            Ok(options)
        })
}
