mod cursor;
mod ddp;
mod drop_handle;
mod ejson;
mod inflights;
mod matcher;
mod mergebox;
mod session;
mod subscriptions;

use crate::subscriptions::Subscriptions;
use anyhow::Error;
use mongodb::Client;
use session::start_session;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::{main, spawn};
use tokio_tungstenite::{accept_async, connect_async};

#[main]
async fn main() -> Result<(), Error> {
    let mut session_id_counter = 0;
    let listener = TcpListener::bind("127.0.0.1:4000").await?;
    let database =
        Client::with_uri_str("mongodb://127.0.0.1:3001/?directConnection=true&maxPoolSize=100")
            .await?
            .database("meteor");
    let subscriptions = Arc::new(Mutex::new(Subscriptions::new(database)));

    loop {
        // Get next ID.
        session_id_counter += 1;
        let session_id = session_id_counter;

        // Wait for next connection and spawn a dedicated task for it.
        let stream = listener.accept().await?.0;
        let subscriptions = subscriptions.clone();
        spawn(async move {
            let client = accept_async(stream).await?;
            let server = connect_async("ws://127.0.0.1:3000/websocket").await?.0;
            let result = start_session(session_id, subscriptions.clone(), client, server).await;
            println!("\n\n\n\n\n{result:?}\n\n\n\n\n");
            result
        });
    }
}
