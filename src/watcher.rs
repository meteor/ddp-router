use anyhow::Error;
use bson::{doc, Document};
use futures_util::{FutureExt, TryStreamExt};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::Database;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub enum Event {
    Clear,
    Delete(Document),
    Insert(Document),
    Update(Document),
}

impl From<ChangeStreamEvent<Document>> for Event {
    fn from(event: ChangeStreamEvent<Document>) -> Self {
        match event {
            ChangeStreamEvent {
                operation_type: OperationType::Delete,
                document_key: Some(document_key),
                ..
            } => Self::Delete(document_key),
            ChangeStreamEvent {
                operation_type: OperationType::Drop | OperationType::DropDatabase,
                ..
            } => Self::Clear,
            ChangeStreamEvent {
                operation_type: OperationType::Insert,
                full_document: Some(full_document),
                ..
            } => Self::Insert(full_document),
            ChangeStreamEvent {
                operation_type: OperationType::Update,
                full_document: Some(full_document),
                ..
            } => Self::Update(full_document),
            event => unreachable!("Unexpected event: {event:?}"),
        }
    }
}

pub struct Watcher {
    change_streams: BTreeMap<String, Arc<Mutex<Sender<Event>>>>,
    database: Database,
}

impl Watcher {
    pub fn new(database: Database) -> Self {
        Self {
            change_streams: BTreeMap::new(),
            database,
        }
    }

    fn start(&self, collection: String, sender: Arc<Mutex<Sender<Event>>>) {
        let database = self.database.clone();
        let task = async move {
            // The current Meteor's Oplog tailing has to refetch a document by
            // `_id` when a document outside of the current documents set is
            // updated and it _may_ match the selector now. With Change Streams
            // we can skip that by fetching the full documents.
            // https://github.com/meteor/meteor/blob/7411b3c85a3c95a6b6f3c588babe6eae894d6fb6/packages/mongo/oplog_observe_driver.js#L652
            let pipeline = [
                doc! { "$match": { "operationType": { "$in": ["delete", "drop", "dropDatabase", "insert", "update"] } } },
                doc! { "$project": { "_id": 1, "documentKey": 1, "fullDocument": 1, "ns": 1, "operationType": 1 } },
            ];

            let options = ChangeStreamOptions::builder()
                // TODO: Ideally we would use `Required` here, but it has to be
                // enabled on the database level. It should be configurable.
                .full_document(Some(FullDocumentType::UpdateLookup))
                .build();

            let mut change_stream = database
                .collection(&collection)
                .watch(pipeline, Some(options))
                .await?;

            while let Some(event) = change_stream.try_next().await? {
                // TODO: When all receivers were dropped, we should stop the stream.
                let _ = sender.lock().await.send(event.into());
            }

            Ok::<_, Error>(())
        };

        spawn(task.then(|result| async move {
            // TODO: Better handling of subtasks.
            if let Err(error) = &result {
                println!("\x1b[0;31m[[ERROR]] {error}\x1b[0m");
            }
            result
        }));
    }

    pub async fn watch(&mut self, collection: String) -> Receiver<Event> {
        if let Some(sender) = self.change_streams.get(&collection) {
            return sender.lock().await.subscribe();
        }

        let (sender, receiver) = channel(1024);
        let sender = Arc::new(Mutex::new(sender));
        self.start(collection.clone(), sender.clone());
        self.change_streams.insert(collection, sender);
        receiver
    }
}
