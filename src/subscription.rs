use crate::ddp::DDPMessage;
use crate::mergebox::Mergebox;
use crate::query::Query;
use anyhow::Error;
use mongodb::Database;
use serde_json::Value;

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

impl TryFrom<(&String, &Vec<Value>)> for Subscription {
    type Error = Error;
    fn try_from((id, value): (&String, &Vec<Value>)) -> Result<Self, Self::Error> {
        let queries = value
            .iter()
            .map(Query::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            id: id.clone(),
            initial_results_sent: false,
            queries,
        })
    }
}
