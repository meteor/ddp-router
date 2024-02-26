mod cursor;
mod ddp;
mod drop_handle;
mod ejson;
mod inflights;
mod lookup;
mod matcher;
mod mergebox;
mod projector;
mod session;
mod settings;
mod sorter;
mod subscriptions;
mod watcher;

use anyhow::Error;
use futures_util::FutureExt;
use mongodb::Client;
use session::start_session;
use settings::Settings;
use std::sync::Arc;
use subscriptions::Subscriptions;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::{main, spawn};
use tokio_tungstenite::{accept_async, connect_async};
use watcher::Watcher;

#[main]
async fn main() -> Result<(), Error> {
    let settings = Settings::from("./config")?;
    println!("\x1b[0;33mrouter\x1b[0m Started at {}", settings.router.url);

    let mut session_id_counter = 0;
    let listener = TcpListener::bind(settings.router.url).await?;
    let database = Client::with_uri_str(settings.mongo.url)
        .await?
        .default_database()
        .expect("Mongo URL did not specify the database");
    let watcher = Watcher::new(database.clone());
    let subscriptions = Arc::new(Mutex::new(Subscriptions::new(database, watcher)));

    loop {
        // Get next ID.
        session_id_counter += 1;
        let session_id = session_id_counter;

        // Wait for next connection and spawn a dedicated task for it.
        let stream = listener.accept().await?.0;
        let meteor_url = settings.meteor.url.clone();
        let subscriptions = subscriptions.clone();
        spawn(
            async move {
                let client = accept_async(stream).await?;
                let server = connect_async(meteor_url).await?.0;
                start_session(session_id, subscriptions.clone(), client, server).await
            }
            .then(|result| async move {
                // TODO: Better handling of subtasks.
                if let Err(error) = &result {
                    println!("\x1b[0;31m[[ERROR]] {error:?}\x1b[0m");
                }
                result
            }),
        );
    }
}
