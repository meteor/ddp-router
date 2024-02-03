use crate::ejson::into_ejson_document;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, bail, Error};
use bson::{to_document, Document};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::{options::FindOptions, Database};
use serde_json::{Map, Value};
use std::mem::replace;

pub struct Query {
    collection: String,
    selector: Document,
    options: FindOptions,
    documents: Vec<Map<String, Value>>,
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
        self.fetch(database, mergebox).await
    }

    pub async fn start(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        self.fetch(database, mergebox).await
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
