mod description;
mod fetcher;
mod viewer;

use crate::drop_handle::DropHandle;
use crate::mergebox::{Mergebox, Mergeboxes};
use crate::watcher::Watcher;
use anyhow::Error;
pub use description::CursorDescription;
use fetcher::CursorFetcher;
use futures_util::FutureExt;
use mongodb::Database;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;

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

    pub fn new(
        database: Database,
        description: CursorDescription,
        watcher: Arc<Mutex<Watcher>>,
    ) -> Self {
        let fetcher = CursorFetcher::new(database, description.clone(), watcher);
        Self {
            description,
            mergeboxes: Arc::new(Mutex::new(Mergeboxes::default())),
            fetcher: Arc::new(Mutex::new(fetcher)),
            task: None,
        }
    }

    pub async fn start(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // Register new mergebox. If it is the first one, start the background
        // task. If not, add all already fetched documents to it.
        let is_first = self
            .mergeboxes
            .lock()
            .await
            .insert_mergebox(session_id, mergebox);

        if is_first {
            println!("\x1b[0;33mrouter\x1b[0m start({:?})", self.description);

            // Run initial query.
            let mergeboxes = self.mergeboxes.clone();
            self.fetcher.lock().await.fetch(&mergeboxes).await?;

            // Start background task.
            let fetcher = self.fetcher.clone();
            let task = async move {
                // Start an event processor or fall back to pooling.
                let receiver_or_interval = fetcher.lock().await.watch().await;
                match receiver_or_interval {
                    Ok(mut receiver) => loop {
                        let event = receiver.recv().await?;
                        fetcher.lock().await.process(event, &mergeboxes).await?;
                    },
                    Err(mut interval) => loop {
                        interval.tick().await;
                        fetcher.lock().await.fetch(&mergeboxes).await?;
                    },
                }
            }
            .then(|result| async move {
                // TODO: Better handling of subtasks.
                if let Err(error) = &result {
                    println!("\x1b[0;31m[[ERROR]] {error}\x1b[0m");
                }
                result
            });
            let _ = self.task.insert(DropHandle::new(spawn(task)));
        } else {
            self.fetcher.lock().await.register(mergebox).await?;
        }

        Ok(())
    }

    pub async fn stop(
        &mut self,
        session_id: usize,
        mergebox: &Arc<Mutex<Mergebox>>,
    ) -> Result<(), Error> {
        // Unregister all documents.
        self.fetcher.lock().await.unregister(mergebox).await?;

        // If it is the last one, stop the cursor. If it is the last one, stop
        // the background task.
        let is_last = self.mergeboxes.lock().await.remove_mergebox(session_id);
        if is_last {
            println!("\x1b[0;33mrouter\x1b[0m  stop({:?})", self.description);

            // Shutdown task (if any).
            if let Some(task) = self.task.take() {
                task.shutdown().await;
            }
        }

        Ok(())
    }
}
