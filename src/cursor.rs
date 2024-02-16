use crate::drop_handle::DropHandle;
use crate::ejson::into_ejson_document;
use crate::matcher::DocumentMatcher;
use crate::mergebox::{Mergebox, Mergeboxes};
use crate::projector::Projector;
use crate::sorter as Sorter;
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
    matcher: Option<DocumentMatcher>,
    projector: Option<Projector>,
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
        Self {
            database,
            description,
            documents: Vec::default(),
            matcher: None,
            projector: None,
            watcher,
        }
    }

    async fn process(
        &mut self,
        event: Event,
        mergeboxes: &Arc<Mutex<Mergeboxes>>,
    ) -> Result<(), Error> {
        match event {
            Event::Clear => {
                let mut mergeboxes = mergeboxes.lock().await;
                for mut document in take(&mut self.documents) {
                    let id = extract_id(&mut document)?;
                    mergeboxes
                        .remove(self.description.collection.clone(), id, &document)
                        .await?;
                }
                OK
            }
            Event::Delete(document) => {
                let mut document = into_ejson_document(document);
                let id = extract_id(&mut document)?;
                let index = self
                    .documents
                    .iter()
                    .position(|x| x.get("_id") == Some(&id))
                    .ok_or_else(|| {
                        anyhow!("Document {id} not found in {}", self.description.collection)
                    })?;
                let mut document = self.documents.swap_remove(index);
                document.remove("_id");
                mergeboxes
                    .lock()
                    .await
                    .remove(self.description.collection.clone(), id, &document)
                    .await
            }
            Event::Insert(document) => {
                let mut document = into_ejson_document(document);
                if !self.matcher.as_ref().unwrap().matches(&document) {
                    return OK;
                }

                self.projector.as_ref().unwrap().apply(&mut document);

                self.documents.push(document.clone());
                let id = extract_id(&mut document)?;
                mergeboxes
                    .lock()
                    .await
                    .insert(self.description.collection.clone(), id, document)
                    .await
            }
            Event::Update(document) => {
                let mut document = into_ejson_document(document);
                let is_matching = self.matcher.as_ref().unwrap().matches(&document);
                if is_matching {
                    self.documents.push(document.clone());
                }

                self.projector.as_ref().unwrap().apply(&mut document);

                let id = extract_id(&mut document)?;
                if is_matching {
                    mergeboxes
                        .lock()
                        .await
                        .insert(self.description.collection.clone(), id.clone(), document)
                        .await?;
                }

                let maybe_index = self
                    .documents
                    .iter()
                    .position(|x| x.get("_id") == Some(&id));
                if let Some(index) = maybe_index {
                    let mut document = self.documents.swap_remove(index);
                    document.remove("_id");
                    mergeboxes
                        .lock()
                        .await
                        .remove(self.description.collection.clone(), id, &document)
                        .await?;
                }

                OK
            }
        }
    }

    async fn watch(&mut self) -> Result<Option<Receiver<Event>>, Error> {
        let CursorDescription {
            collection,
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
        } = &self.description;

        // Publication can opt-out of real-time updates.
        if *disable_oplog {
            return Ok(None);
        }

        // We have to understand the selector to process the Change Stream
        // events correctly.
        match DocumentMatcher::compile(selector) {
            Ok(matcher) => self.matcher = Some(matcher),
            Err(error) => {
                println!(
                    "\x1b[0;32mmongo\x1b[0m \x1b[0;31mselector {selector:?} is not supported: {error}\x1b[0m"
                );
                return Ok(None);
            }
        };

        match Projector::compile(projection.as_ref()) {
            Ok(projector) => self.projector = Some(projector),
            Err(error) => {
                println!(
                    "\x1b[0;32mmongo\x1b[0m \x1b[0;31mprojection {projection:?} is not supported: {error}\x1b[0m",
                );
                return Ok(None);
            }
        }

        if !Sorter::is_supported(sort.as_ref()) {
            println!("\x1b[0;32mmongo\x1b[0m \x1b[0;31msort {sort:?} not supported\x1b[0m",);
            return Ok(None);
        }

        // TODO: Implement top-N logic.
        if limit.is_some() || skip.is_some() {
            println!("\x1b[0;32mmongo\x1b[0m \x1b[0;31mlimit and skip are not supported\x1b[0m");
            return Ok(None);
        }

        let receiver = self.watcher.lock().await.watch(collection.clone()).await;
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

fn extract_id(document: &mut Map<String, Value>) -> Result<Value, Error> {
    document
        .remove("_id")
        .ok_or_else(|| anyhow!("_id not found in {document:?}"))
}
