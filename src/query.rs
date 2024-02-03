use crate::ejson::into_ejson_document;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, bail, Error};
use bson::{doc, to_document, Document};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::change_stream::ChangeStream;
use mongodb::options::{ChangeStreamOptions, FindOptions};
use mongodb::Database;
use serde_json::{Map, Value};
use std::mem::{replace, take};
use std::time::Duration;

pub struct Query {
    collection: String,
    selector: Document,
    options: FindOptions,
    documents: Vec<Map<String, Value>>,
    change_stream: Option<ChangeStream<ChangeStreamEvent<Document>>>,
}

impl Query {
    async fn fetch(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        let mut documents: Vec<_> = database
            .collection::<Document>(&self.collection)
            .find(Some(self.selector.clone()), Some(self.options.clone()))
            .await?
            .map(|maybe_document| maybe_document.map(into_ejson_document))
            .try_collect()
            .await?;

        for document in &mut documents {
            let id = document
                .remove("_id")
                .ok_or_else(|| anyhow!("_id not found in {document:?}"))?;
            mergebox.insert(self.collection.clone(), id.clone(), document.clone())?;
            document.insert(String::from("_id"), id);
        }

        for mut document in replace(&mut self.documents, documents) {
            let id = document
                .remove("_id")
                .ok_or_else(|| anyhow!("_id not found in {document:?}"))?;
            mergebox.remove(self.collection.clone(), id, &document)?;
        }

        Ok(())
    }

    pub async fn pool(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        if let Some(change_stream) = &mut self.change_stream {
            while let Some(event) = change_stream.next_if_any().await? {
                match event {
                    ChangeStreamEvent {
                        operation_type: OperationType::Delete,
                        document_key: Some(document),
                        ..
                    } => {
                        let mut document = into_ejson_document(document);
                        let id = document
                            .remove("_id")
                            .ok_or_else(|| anyhow!("_id not found in {document:?}"))?;
                        let index = self.documents.iter().position(|x| x.get("_id") == Some(&id)).ok_or_else(|| anyhow!("Document {id} not found"))?;
                        let mut document = self.documents.swap_remove(index);
                        document.remove("_id");
                        mergebox.remove(self.collection.clone(), id, &document)?;
                    }
                    ChangeStreamEvent {
                        operation_type: OperationType::Drop | OperationType::DropDatabase,
                        ..
                    } => {
                        for mut document in take(&mut self.documents) {
                            let id = document
                                .remove("_id")
                                .ok_or_else(|| anyhow!("_id not found in {document:?}"))?;
                            mergebox.remove(self.collection.clone(), id, &document)?;
                        }
                    }
                    ChangeStreamEvent {
                        operation_type: OperationType::Insert,
                        full_document: Some(document),
                        ..
                    } => {
                        let mut document = into_ejson_document(document);
                        self.documents.push(document.clone());
                        let id = document
                            .remove("_id")
                            .ok_or_else(|| anyhow!("_id not found in {document:?}"))?;
                        mergebox.insert(self.collection.clone(), id, document)?;
                    }
                    // TODO: Handle other events.
                    _ => todo!("{event:?}"),
                }
            }

            Ok(())
        } else {
            self.fetch(database, mergebox).await
        }
    }

    pub async fn start(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        self.fetch(database, mergebox).await?;
        self.start_change_stream(database).await
    }

    pub async fn start_change_stream(&mut self, database: &mut Database) -> Result<(), Error> {
        // TODO: Both `limit` and `skip` will require a proper in-memory sorting.
        if self.options.limit.is_none() && self.options.skip.is_none() {
            let mut pipeline = vec![doc! { "$match": { "$expr": self.selector.clone() } }];
            if let Some(projection) = &self.options.projection {
                pipeline.push(doc! { "$project": projection.clone() });
            }

            let mut options = ChangeStreamOptions::default();
            options.max_await_time = Some(Duration::from_millis(0));

            let change_stream = database
                .collection::<Document>(&self.collection)
                .watch(pipeline, Some(options))
                .await?;
            self.change_stream = Some(change_stream);
        }

        Ok(())
    }

    pub async fn stop(self, mergebox: &mut Mergebox) -> Result<(), Error> {
        self.documents.into_iter().try_for_each(|mut document| {
            let id = document
                .remove("_id")
                .ok_or_else(|| anyhow!("_id not found in {document:?}"))?;
            mergebox.remove(self.collection.clone(), id, &document)
        })
    }
}

impl TryFrom<&Value> for Query {
    type Error = Error;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
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
            collection: collection.clone(),
            selector,
            options,
            documents: Vec::default(),
            change_stream: None,
        })
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
