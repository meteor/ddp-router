use crate::ddp::DDPMessage;
use crate::ejson::IntoEjson;
use crate::mergebox::Mergebox;
use anyhow::{anyhow, bail, Error};
use bson::{to_document, Document};
use futures_util::TryStreamExt;
use mongodb::{options::FindOptions, Database};
use serde_json::Value;
use std::mem::replace;

pub struct Subscription {
    id: String,
    initial_results_sent: bool,
    queries: Vec<Query>,
}

impl Subscription {
    pub async fn fetch(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        for query in &mut self.queries {
            query.fetch(database, mergebox).await?;
        }

        if !self.initial_results_sent {
            self.initial_results_sent = true;
            // TODO: This should be handled elsewhere.
            mergebox.messages.push(DDPMessage::Ready {
                subs: vec![self.id.clone()],
            });
        }

        Ok(())
    }

    pub fn stop(self, mergebox: &mut Mergebox) -> Result<(), Error> {
        self.queries
            .into_iter()
            .try_for_each(|query| query.stop(mergebox))?;
        // TODO: This should be handled elsewhere.
        mergebox.messages.push(DDPMessage::Nosub {
            id: self.id,
            error: None,
        });
        Ok(())
    }
}

impl TryFrom<(&String, &mut Vec<Value>)> for Subscription {
    type Error = Error;
    fn try_from((id, value): (&String, &mut Vec<Value>)) -> Result<Self, Self::Error> {
        let queries = value
            .iter_mut()
            .map(Query::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            id: id.clone(),
            initial_results_sent: false,
            queries,
        })
    }
}

struct Query {
    pub collection: String,
    pub selector: Document,
    pub options: FindOptions,
    pub documents: Vec<Document>,
}

impl Query {
    pub async fn fetch(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        let mut documents: Vec<_> = database
            .collection::<Document>(&self.collection)
            .find(Some(self.selector.clone()), Some(self.options.clone()))
            .await?
            .try_collect()
            .await?;

        for document in &mut documents {
            let id = document
                .remove("_id")
                .ok_or_else(|| anyhow!("_id not found in {document}"))?;
            mergebox.insert(&self.collection, id.clone().into_ejson(), document.clone())?;
            document.insert("_id", id);
        }

        for mut document in replace(&mut self.documents, documents) {
            let id = document
                .remove("_id")
                .ok_or_else(|| anyhow!("_id not found in {document}"))?
                .into_ejson();
            mergebox.remove(&self.collection, id, &document)?;
        }

        Ok(())
    }

    pub fn stop(self, mergebox: &mut Mergebox) -> Result<(), Error> {
        self.documents.into_iter().try_for_each(|mut document| {
            let id = document
                .remove("_id")
                .ok_or_else(|| anyhow!("_id not found in {document}"))?
                .into_ejson();
            mergebox.remove(&self.collection, id, &document)
        })
    }
}

impl TryFrom<&mut Value> for Query {
    type Error = Error;
    fn try_from(value: &mut Value) -> Result<Self, Self::Error> {
        let Value::String(collection) = value
            .get_mut("collectionName")
            .ok_or_else(|| anyhow!("Missing collectionName"))?
            .take()
        else {
            bail!("Incorrect collectionName (expected a string)");
        };

        let selector = to_document(
            value
                .get_mut("selector")
                .ok_or_else(|| anyhow!("Missing selector"))?,
        )?;

        let options = to_options(
            value
                .get_mut("options")
                .ok_or_else(|| anyhow!("Missing options"))?
                .take(),
        )?;

        Ok(Self {
            collection,
            selector,
            options,
            documents: Vec::default(),
        })
    }
}

fn to_options(options: Value) -> Result<FindOptions, Error> {
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
