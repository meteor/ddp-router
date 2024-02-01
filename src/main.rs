mod ddp;
mod ejson;
mod mergebox;
mod session;
mod subscription;

use anyhow::Error;
use mongodb::Client;
use session::Session;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, connect_async};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:4000").await?;
    let database = Client::with_uri_str("mongodb://127.0.0.1:3001/?directConnection=true")
        .await?
        .database("meteor");
    loop {
        let stream = listener.accept().await?.0;
        let database = database.clone();
        tokio::spawn(async move {
            let client = accept_async(stream).await?;
            let server = connect_async("ws://127.0.0.1:3000/websocket").await?.0;
            let result = Session::new(database, client, server).start().await;
            println!("\n\n\n\n\n{result:?}\n\n\n\n\n");
            result
        });
    }
}
