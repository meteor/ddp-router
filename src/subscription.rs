use crate::ddp::DDPMessage;
use crate::mergebox::Mergebox;
use crate::query::Query;
use anyhow::Error;
use mongodb::Database;
use serde_json::Value;

pub struct Subscription {
    id: String,
    queries: Vec<Query>,
}

impl Subscription {
    pub async fn pool(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        for query in &mut self.queries {
            query.pool(database, mergebox).await?;
        }

        Ok(())
    }

    pub async fn start(
        &mut self,
        database: &mut Database,
        mergebox: &mut Mergebox,
    ) -> Result<(), Error> {
        for query in &mut self.queries {
            query.start(database, mergebox).await?;
        }

        // TODO: This should be handled elsewhere.
        mergebox.messages.push(DDPMessage::Ready {
            subs: vec![self.id.clone()],
        });

        Ok(())
    }

    pub async fn stop(self, mergebox: &mut Mergebox) -> Result<(), Error> {
        for query in self.queries {
            query.stop(mergebox).await?;
        }

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
            queries,
        })
    }
}
