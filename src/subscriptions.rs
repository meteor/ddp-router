use crate::inflights::Inflight;
use crate::mergebox::Mergebox;
use crate::query::Query;
use anyhow::{anyhow, Error};
use mongodb::Database;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Subscriptions {
    database: Database,
    mergebox: Arc<Mutex<Mergebox>>,
    queries_map: BTreeMap<String, Vec<Arc<Query>>>,
}

impl Subscriptions {
    fn find_same_query(&self, query: &Query) -> Option<Arc<Query>> {
        self.queries_map
            .values()
            .flatten()
            .find(|other| ***other == *query)
            .cloned()
    }

    pub fn new(database: Database, mergebox: Arc<Mutex<Mergebox>>) -> Self {
        Self {
            database,
            mergebox,
            queries_map: BTreeMap::default(),
        }
    }

    pub async fn start(
        &mut self,
        inflight: &Inflight,
        id: &str,
        error: &Option<Value>,
        result: &Option<Value>,
    ) -> Result<(), Error> {
        match (error, result) {
            (Some(error), _) => match error.get("reason") {
                Some(Value::String(message))
                    if Some(message)
                        .and_then(|x| x.strip_prefix("Method '__subscription__"))
                        .and_then(|x| x.strip_suffix("' not found"))
                        .is_some_and(|x| x == inflight.name) =>
                {
                    Err(anyhow!(
                        "Publication for {} was not registered",
                        inflight.name
                    ))
                }
                _ => Err(anyhow!(error.clone())),
            },
            (None, Some(Value::Array(cursor_descriptions))) => {
                let mut queries = vec![];
                for mut query in cursor_descriptions
                    .iter()
                    .map(|value| Query::try_from((&self.database, value)))
                    .collect::<Result<Vec<_>, _>>()?
                {
                    if let Some(query) = self.find_same_query(&query) {
                        queries.push(query);
                    } else {
                        query.start(&self.mergebox).await?;
                        queries.push(Arc::new(query));
                    }
                }
                self.queries_map.insert(id.to_owned(), queries);
                Ok(())
            }
            (None, _) => Err(anyhow!("Incorrect format")),
        }
    }

    pub async fn stop(&mut self, id: &str) -> Result<Option<String>, Error> {
        if let Some((id, queries)) = self.queries_map.remove_entry(id) {
            for query in queries {
                if let Some(query) = Arc::into_inner(query) {
                    query.stop(&self.mergebox).await?;
                }
            }
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}
