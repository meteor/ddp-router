use crate::drop_handle::DropHandle;
use crate::ejson::into_ejson_document;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, bail, Error};
use bson::{doc, to_document, Document};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::options::FindOptions;
use mongodb::Database;
use serde_json::{Map, Value};
use std::mem::{replace, take};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;

const OK: Result<(), Error> = Ok(());

pub struct Query {
    database: Database,
    collection: String,
    selector: Document,
    options: FindOptions,
    documents: Arc<Mutex<Vec<Map<String, Value>>>>,
    change_stream: Option<DropHandle<Result<(), Error>>>,
}

impl Query {
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

        for mut document in replace(&mut *self.documents.lock().await, documents) {
            let id = extract_id(&mut document)?;
            mergebox
                .remove(self.collection.clone(), id, &document)
                .await?;
        }

        OK
    }

    pub async fn pool(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        if self.change_stream.is_none() {
            self.fetch(mergebox).await?;
        }

        OK
    }

    pub async fn start(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        self.fetch(mergebox).await?;
        self.start_change_stream(mergebox).await?;
        OK
    }

    pub async fn start_change_stream(
        &mut self,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // TODO: Both `limit` and `skip` will require a proper in-memory sorting.
        if self.options.limit.is_none() && self.options.skip.is_none() {
            let mut pipeline = vec![doc! { "$match": { "$expr": self.selector.clone() } }];
            if let Some(projection) = &self.options.projection {
                pipeline.push(doc! { "$project": projection.clone() });
            }

            let collection = self.collection.clone();
            let database = self.database.clone();
            let documents = self.documents.clone();
            let mergebox = mergebox.clone();

            self.change_stream = Some(DropHandle::new(spawn(async move {
                let mut change_stream = database
                    .collection::<Document>(&collection)
                    .watch(pipeline, None)
                    .await?;

                while let Some(event) = change_stream.try_next().await? {
                    process_change_stream_event(event, &collection, &documents, &mergebox).await?;
                }

                OK
            })));
        }

        OK
    }

    pub async fn stop(mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        // Shutdown Change Stream (if any).
        if let Some(handle) = self.change_stream.take() {
            handle.shutdown().await;
        }

        // Unregister all documents.
        let documents = Arc::into_inner(self.documents)
            .ok_or_else(|| anyhow!("Failed to consume Query.documents"))?
            .into_inner();

        let mut mergebox = mergebox.lock().await;
        for mut document in documents {
            let id = extract_id(&mut document)?;
            mergebox
                .remove(self.collection.clone(), id, &document)
                .await?;
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

        let options = to_options(
            value
                .get("options")
                .ok_or_else(|| anyhow!("Missing options"))?,
        )?;

        Ok(Self {
            database: database.clone(),
            collection: collection.clone(),
            selector,
            options,
            documents: Arc::default(),
            change_stream: None,
        })
    }
}

fn extract_id(document: &mut Map<String, Value>) -> Result<Value, Error> {
    document
        .remove("_id")
        .ok_or_else(|| anyhow!("_id not found in {document:?}"))
}

async fn process_change_stream_event(
    event: ChangeStreamEvent<Document>,
    collection: &str,
    documents: &Arc<Mutex<Vec<Map<String, Value>>>>,
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
            let index = documents
                .lock()
                .await
                .iter()
                .position(|x| x.get("_id") == Some(&id))
                .ok_or_else(|| anyhow!("Document {id} not found"))?;
            let mut document = documents.lock().await.swap_remove(index);
            document.remove("_id");
            mergebox
                .lock()
                .await
                .remove(collection.to_owned(), id, &document)
                .await
        }
        ChangeStreamEvent {
            operation_type: OperationType::Drop | OperationType::DropDatabase,
            ..
        } => {
            for mut document in take(&mut *documents.lock().await) {
                let id = extract_id(&mut document)?;
                mergebox
                    .lock()
                    .await
                    .remove(collection.to_owned(), id, &document)
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
            documents.lock().await.push(document.clone());
            let id = extract_id(&mut document)?;
            mergebox
                .lock()
                .await
                .insert(collection.to_owned(), id, document)
                .await
        }
        // TODO: Handle other events.
        _ => todo!("{event:?}"),
    }
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
