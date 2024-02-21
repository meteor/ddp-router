use crate::drop_handle::DropHandle;
use crate::ejson::into_ejson_document;
use crate::matcher::DocumentMatcher;
use crate::mergebox::{Mergebox, Mergeboxes};
use crate::projector::Projector;
use crate::sorter::Sorter;
use crate::watcher::{Event, Watcher};
use anyhow::{anyhow, Error};
use bson::{doc, Document};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use mongodb::options::FindOptions;
use mongodb::Database;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::mem::{replace, take};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::time::{interval_at, Duration, Instant, Interval};

// TODO: The structure hierarchy and naming is just **terrible** here.

const OK: Result<(), Error> = Ok(());

pub struct Cursor {
    description: CursorDescription,
    mergeboxes: Arc<Mutex<Mergeboxes>>,
    fetcher: Arc<Mutex<CursorFetcher>>,
    task: Option<DropHandle<Result<(), Error>>>,
}

impl Cursor {
    pub fn description(&self) -> &CursorDescription {
        &self.description
    }

    pub async fn start(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // Register new mergebox. If it is the first one, start the background
        // task. If not, add all already fetched documents to it.
        let is_first = {
            self.mergeboxes
                .lock()
                .await
                .insert_mergebox(session_id, mergebox)
        };
        if is_first {
            println!("\x1b[0;33mrouter\x1b[0m start({:?})", self.description);

            // Run initial query.
            let mergeboxes = self.mergeboxes.clone();
            self.fetcher.lock().await.fetch(&mergeboxes).await?;

            // Start background task.
            let fetcher = self.fetcher.clone();
            let task = async move {
                // Separate context to eliminate locking.
                let maybe_receiver = { fetcher.lock().await.watch().await? };

                // Start an event processor or fall back to pooling.
                if let Some(mut receiver) = maybe_receiver {
                    loop {
                        let event = receiver.recv().await?;
                        fetcher.lock().await.process(event, &mergeboxes).await?;
                    }
                } else {
                    let mut timer = { fetcher.lock().await.interval() };
                    loop {
                        timer.tick().await;
                        fetcher.lock().await.fetch(&mergeboxes).await?;
                    }
                }
            }
            // TODO: Better handling of subtasks.
            .then(|result| async move {
                if let Err(error) = &result {
                    println!("\x1b[0;31m[[ERROR]] {error}\x1b[0m");
                }
                result
            });
            let _ = self.task.insert(DropHandle::new(spawn(task)));
        } else {
            let documents = { self.fetcher.lock().await.documents.clone() };

            let mut mergebox = mergebox.lock().await;
            for mut document in documents {
                let id = extract_id(&mut document)?;
                mergebox
                    .insert(self.description.collection.clone(), id, document)
                    .await?;
            }
        }

        OK
    }

    pub async fn stop(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // Unregister all documents.
        {
            let documents = { self.fetcher.lock().await.documents.clone() };
            let mut mergebox = mergebox.lock().await;
            for mut document in documents {
                let id = extract_id(&mut document)?;
                mergebox
                    .remove(self.description.collection.clone(), id, &document)
                    .await?;
            }
        }

        // If it is the last one, stop the cursor. If it is the last one, stop
        // the background task.
        let is_last = { self.mergeboxes.lock().await.remove_mergebox(session_id) };
        if is_last {
            println!("\x1b[0;33mrouter\x1b[0m  stop({:?})", self.description);

            // Shutdown task (if any).
            if let Some(task) = self.task.take() {
                task.shutdown().await;
            }
        }

        OK
    }
}

impl From<CursorFetcher> for Cursor {
    fn from(fetcher: CursorFetcher) -> Self {
        Self {
            description: fetcher.description.clone(),
            mergeboxes: Arc::new(Mutex::new(Mergeboxes::default())),
            fetcher: Arc::new(Mutex::new(fetcher)),
            task: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CursorDescription {
    #[serde(rename = "collectionName")]
    pub collection: String,
    pub selector: Document,
    pub options: CursorOptions,
}

pub struct CursorFetcher {
    database: Database,
    description: CursorDescription,
    documents: Vec<Map<String, Value>>,
    viewer: Option<CursorViewer>,
    watcher: Arc<Mutex<Watcher>>,
}

impl CursorFetcher {
    async fn fetch(&mut self, mergeboxes: &Arc<Mutex<Mergeboxes>>) -> Result<(), Error> {
        println!("\x1b[0;32mmongo\x1b[0m  fetch({:?})", self.description);

        let mut documents: Vec<_> = self
            .database
            .collection::<Document>(&self.description.collection)
            .find(
                Some(self.description.selector.clone()),
                Some(self.description.options.clone().into()),
            )
            .await?
            .map(|maybe_document| maybe_document.map(into_ejson_document))
            .try_collect()
            .await?;

        let mut mergeboxes = mergeboxes.lock().await;

        for document in &mut documents {
            let id = extract_id(document)?;
            mergeboxes
                .insert(
                    self.description.collection.clone(),
                    id.clone(),
                    document.clone(),
                )
                .await?;
            document.insert(String::from("_id"), id);
        }

        for mut document in replace(&mut self.documents, documents) {
            let id = extract_id(&mut document)?;
            mergeboxes
                .remove(self.description.collection.clone(), id.clone(), &document)
                .await?;
        }

        OK
    }

    fn interval(&self) -> Interval {
        let interval = Duration::from_millis(self.description.options.polling_interval_ms);
        interval_at(Instant::now() + interval, interval)
    }

    pub fn new(
        database: Database,
        description: CursorDescription,
        watcher: Arc<Mutex<Watcher>>,
    ) -> Self {
        let viewer = match CursorViewer::try_from(&description) {
            Ok(viewer) => Some(viewer),
            Err(error) => {
                println!("\x1b[0;32mmongo\x1b[0m \x1b[0;31m{error}\x1b[0m");
                None
            }
        };

        Self {
            database,
            description,
            documents: Vec::default(),
            viewer,
            watcher,
        }
    }

    async fn process(
        &mut self,
        event: Event,
        mergeboxes: &Arc<Mutex<Mergeboxes>>,
    ) -> Result<(), Error> {
        let refetch = process(
            event,
            &self.description,
            &mut self.documents,
            mergeboxes,
            self.viewer.as_ref().unwrap(),
        )
        .await?;
        if refetch {
            self.fetch(mergeboxes).await?;
        }

        OK
    }

    async fn watch(&mut self) -> Result<Option<Receiver<Event>>, Error> {
        let receiver = self
            .watcher
            .lock()
            .await
            .watch(self.description.collection.clone())
            .await;
        Ok(Some(receiver))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CursorOptions {
    #[serde(default, rename = "disableOplog")]
    pub disable_oplog: bool,
    pub limit: Option<i64>,
    #[serde(
        default = "CursorOptions::default_polling_interval_ms",
        rename = "pollingIntervalMs"
    )]
    pub polling_interval_ms: u64,
    pub projection: Option<Document>,
    pub skip: Option<u64>,
    pub sort: Option<Document>,
    pub transform: Option<()>,
}

impl CursorOptions {
    fn default_polling_interval_ms() -> u64 {
        10_000
    }

    fn limit(&self) -> Option<usize> {
        self.limit.map(|limit| limit.unsigned_abs() as usize)
    }
}

impl From<CursorOptions> for FindOptions {
    fn from(options: CursorOptions) -> Self {
        Self::builder()
            .limit(options.limit)
            .projection(options.projection)
            .skip(options.skip)
            .sort(options.sort)
            .build()
    }
}

struct CursorViewer {
    matcher: DocumentMatcher,
    projector: Projector,
    sorter: Sorter,
}

impl TryFrom<&CursorDescription> for CursorViewer {
    type Error = Error;
    fn try_from(description: &CursorDescription) -> Result<Self, Self::Error> {
        let CursorDescription {
            selector,
            options:
                CursorOptions {
                    disable_oplog,
                    limit,
                    projection,
                    skip,
                    sort,
                    ..
                },
            ..
        } = description;

        // Publication can opt-out of real-time updates.
        if *disable_oplog {
            return Err(anyhow!("explicitly disabled"));
        }

        // We have to understand the selector to process the Change Stream
        // events correctly.
        let matcher = DocumentMatcher::compile(selector)
            .map_err(|error| anyhow!("selector {selector:?} is not supported: {error}"))?;

        let projector = Projector::compile(projection.as_ref())
            .map_err(|error| anyhow!("projection {projection:?} is not supported: {error}"))?;

        let sorter = Sorter::compile(sort.as_ref())
            .map_err(|error| anyhow!("sort {sort:?} is not supported: {error}"))?;

        if limit.is_some() && sort.is_none() {
            return Err(anyhow!("limit without sort is not supported"));
        }

        if skip.is_some() {
            return Err(anyhow!("skip is not supported"));
        }

        Ok(Self {
            matcher,
            projector,
            sorter,
        })
    }
}

fn extract_id(document: &mut Map<String, Value>) -> Result<Value, Error> {
    document
        .remove("_id")
        .ok_or_else(|| anyhow!("_id not found in {document:?}"))
}

async fn process(
    event: Event,
    description: &CursorDescription,
    documents: &mut Vec<Map<String, Value>>,
    mergeboxes: &Arc<Mutex<Mergeboxes>>,
    viewer: &CursorViewer,
) -> Result<bool, Error> {
    match event {
        Event::Clear => {
            let mut mergeboxes = mergeboxes.lock().await;
            for mut document in take(documents) {
                let id = extract_id(&mut document)?;
                viewer.projector.apply(&mut document);
                mergeboxes
                    .remove(description.collection.clone(), id, &document)
                    .await?;
            }
            Ok(false)
        }
        Event::Delete(document) => {
            let mut document = into_ejson_document(document);
            let id = extract_id(&mut document)?;
            let Some(index) = documents.iter().position(|x| x.get("_id") == Some(&id)) else {
                return Ok(false);
            };

            if description
                .options
                .limit()
                .is_some_and(|limit| limit == documents.len())
            {
                return Ok(true);
            }

            let mut document = documents.swap_remove(index);
            document.remove("_id");
            viewer.projector.apply(&mut document);
            mergeboxes
                .lock()
                .await
                .remove(description.collection.clone(), id, &document)
                .await?;

            Ok(false)
        }
        Event::Insert(document) => {
            let mut document = into_ejson_document(document);
            if !viewer.matcher.matches(&document) {
                return Ok(false);
            }

            if let Some(limit) = description.options.limit {
                let index = documents
                    .binary_search_by(|x| viewer.sorter.cmp(x, &document))
                    .unwrap_or_else(|index| index);
                // TODO: Safe comparison.
                if index == limit.unsigned_abs() as usize {
                    return Ok(false);
                }

                documents.insert(index, document.clone());
            } else {
                documents.push(document.clone());
            }

            let id = extract_id(&mut document)?;
            viewer.projector.apply(&mut document);
            let mut mergeboxes = mergeboxes.lock().await;
            mergeboxes
                .insert(description.collection.clone(), id, document)
                .await?;

            if let Some(limit) = description.options.limit {
                // TODO: Safe comparison.
                if documents.len() > limit.unsigned_abs() as usize {
                    if let Some(mut document) = documents.pop() {
                        let id = extract_id(&mut document)?;
                        viewer.projector.apply(&mut document);
                        mergeboxes
                            .remove(description.collection.clone(), id, &document)
                            .await?;
                    }
                }
            }

            Ok(false)
        }
        Event::Update(document) => {
            let mut document = into_ejson_document(document);
            let is_matching = viewer.matcher.matches(&document);
            if is_matching {
                if let Some(limit) = description.options.limit {
                    let index = documents
                        .binary_search_by(|x| viewer.sorter.cmp(x, &document))
                        .unwrap_or_else(|index| index);
                    // TODO: Safe comparison.
                    if index == limit.unsigned_abs() as usize {
                        return Ok(false);
                    }

                    documents.insert(index, document.clone());
                } else {
                    documents.push(document.clone());
                }

                let id = extract_id(&mut document)?;
                viewer.projector.apply(&mut document);
                let mut mergeboxes = mergeboxes.lock().await;
                mergeboxes
                    .insert(description.collection.clone(), id.clone(), document)
                    .await?;

                let maybe_index = documents.iter().position(|x| x.get("_id") == Some(&id));
                if let Some(index) = maybe_index {
                    let mut document = documents.swap_remove(index);
                    document.remove("_id");
                    mergeboxes
                        .remove(description.collection.clone(), id, &document)
                        .await?;
                }
            } else {
                let id = extract_id(&mut document)?;
                let Some(index) = documents.iter().position(|x| x.get("_id") == Some(&id)) else {
                    return Ok(false);
                };

                // TODO: Safe comparison.
                if description
                    .options
                    .limit()
                    .is_some_and(|limit| limit == documents.len())
                {
                    return Ok(true);
                }

                let mut document = documents.swap_remove(index);
                let id = extract_id(&mut document)?;
                mergeboxes
                    .lock()
                    .await
                    .remove(description.collection.clone(), id, &document)
                    .await?;
            }

            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{process, CursorDescription, CursorViewer};
    use crate::ddp::DDPMessage;
    use crate::mergebox::{Mergebox, Mergeboxes};
    use crate::watcher::Event;
    use anyhow::Error;
    use bson::doc;
    use serde::Deserialize;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio::sync::mpsc::channel;
    use tokio::sync::Mutex;
    use tokio::test;

    async fn simulate(
        description: Value,
        events: Vec<Event>,
        messages: Vec<DDPMessage>,
    ) -> Result<(), Error> {
        let description = CursorDescription::deserialize(description)?;
        let viewer = CursorViewer::try_from(&description)?;
        let (sender, mut receiver) = channel(64);
        let mergeboxes = Arc::new(Mutex::new({
            let mut mergeboxes = Mergeboxes::default();
            mergeboxes.insert_mergebox(1, &Arc::new(Mutex::new(Mergebox::new(sender))));
            mergeboxes
        }));

        let mut documents = Vec::new();
        for event in events {
            process(event, &description, &mut documents, &mergeboxes, &viewer).await?;
        }

        for message in messages {
            assert_eq!(receiver.try_recv(), Ok(message));
        }

        assert!(receiver.try_recv().is_err());
        assert!(documents.is_empty());

        Ok(())
    }

    macro_rules! simulate {
        ($name:ident, $description:expr, $events:expr, $messages:expr) => {
            #[test]
            async fn $name() -> Result<(), Error> {
                simulate($description, $events, $messages).await
            }
        };
    }

    macro_rules! json_doc {
        ($($json:tt)*) => {
            match json! {{ $($json)* }} {
                Value::Object(map) => map,
                _ => unreachable!(),
            }
        };
    }

    simulate!(
        scenario_1,
        json! {{"collectionName": "x", "selector": {}, "options": {}}},
        vec![],
        vec![]
    );

    simulate!(
        scenario_2,
        json! {{"collectionName": "x", "selector": {}, "options": {}}},
        vec![
            Event::Insert(doc! {"_id": 1}),
            Event::Insert(doc! {"_id": 2, "a": 3}),
            Event::Clear
        ],
        vec![
            DDPMessage::Added {
                collection: "x".to_owned(),
                id: json!(1),
                fields: None
            },
            DDPMessage::Added {
                collection: "x".to_owned(),
                id: json!(2),
                fields: Some(json_doc! {"a": 3})
            },
            DDPMessage::Removed {
                collection: "x".to_owned(),
                id: json!(1)
            },
            DDPMessage::Removed {
                collection: "x".to_owned(),
                id: json!(2)
            }
        ]
    );
}
