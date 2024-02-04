use crate::mergebox::Mergebox;
use crate::query::Query;
use anyhow::Error;
use mongodb::Database;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Subscription {
    pub id: String,
    queries: Vec<Query>,
}

impl Subscription {
    pub async fn start(&mut self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<(), Error> {
        for query in &mut self.queries {
            query.start(mergebox).await?;
        }

        Ok(())
    }

    pub async fn stop(self, mergebox: &Arc<Mutex<Mergebox>>) -> Result<String, Error> {
        for query in self.queries {
            query.stop(mergebox).await?;
        }

        Ok(self.id)
    }
}

impl TryFrom<(&Database, &String, &Vec<Value>)> for Subscription {
    type Error = Error;
    fn try_from(
        (database, id, value): (&Database, &String, &Vec<Value>),
    ) -> Result<Self, Self::Error> {
        let id = id.clone();
        let queries = value
            .iter()
            .map(|value| Query::try_from((database, value)))
            .collect::<Result<_, _>>()?;
        Ok(Self { id, queries })
    }
}
