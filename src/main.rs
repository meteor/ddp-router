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
use dotenvy::dotenv;
use mongodb::Client;
use session::start_session;
use std::env::var;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::{main, spawn};
use tokio_tungstenite::{accept_async, connect_async};
use watcher::Watcher;

#[main]
async fn main() -> Result<(), Error> {
    // It is ok if .env file is not present.
    dotenv().ok();
    let meteor_url = var("METEOR_URL").expect("METEOR_URL is required");
    let mongo_url = var("MONGO_URL").expect("MONGO_URL is required");
    let router_url = var("ROUTER_URL").expect("ROUTER_URL is required");

    println!("\x1b[0;36m Starting DDP-router at:\x1b[0m {} ", router_url);

    let mut session_id_counter = 0;
    let listener = TcpListener::bind(router_url).await?;
    let database = Client::with_uri_str(mongo_url).await?.database("meteor");
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
