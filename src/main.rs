mod cursor;
mod ddp;
mod drop_handle;
mod ejson;
mod inflights;
mod matcher;
mod mergebox;
mod projector;
mod session;
mod sorter;
mod subscriptions;
mod watcher;

use crate::subscriptions::Subscriptions;

use anyhow::Error;
use config::Config;
use mongodb::Client;
use session::start_session;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::{main, spawn};
use tokio_tungstenite::{accept_async, connect_async};
use watcher::Watcher;

#[main]
async fn main() -> Result<(), Error> {
    let settings = Config::builder()
        .add_source(config::File::with_name("./config/Default"))
        .add_source(config::File::with_name("./config/Local").required(false))
        .build()?;

    let meteor_url = settings
        .get_string("METEOR_URL")
        .expect("METEOR_URL is required");

    let mongo_url = settings
        .get_string("MONGO_URL")
        .expect("MONGO_URL is required");

    let router_url = settings
        .get_string("ROUTER_URL")
        .expect("ROUTER_URL is required");

    println!("\x1b[0;36m Starting DDP-router at:\x1b[0m {} ", router_url);

    let mut session_id_counter = 0;
    let listener = TcpListener::bind(router_url).await?;
    let database = Client::with_uri_str(mongo_url)
        .await?
        .default_database()
        .expect("MONGO_URL did not specify the database");
    let watcher = Watcher::new(database.clone());
    let subscriptions = Arc::new(Mutex::new(Subscriptions::new(database, watcher)));

    loop {
        // Get next ID.
        session_id_counter += 1;
        let session_id = session_id_counter;

        // Wait for next connection and spawn a dedicated task for it.
        let stream = listener.accept().await?.0;
        let meteor_url = meteor_url.clone();
        let subscriptions = subscriptions.clone();
        spawn(async move {
            let client = accept_async(stream).await?;
            let server = connect_async(meteor_url).await?.0;
            let result = start_session(session_id, subscriptions.clone(), client, server).await;
            println!("\n\n\n\n\n{result:?}\n\n\n\n\n");
            result
        });
    }
}
