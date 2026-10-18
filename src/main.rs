use anyhow::Result;
use clap::Parser;
use futures_lite::StreamExt;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, endpoint::presets, protocol::Router};
use iroh_gossip::api::GossipSender;
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::Gossip,
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{collections::HashMap, fs, thread};
use tokio::sync::mpsc;

// скомпилировать на телефоне и мобильном

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long)]
    name: String,
}

#[derive(Parser, Debug)]
enum Command {
    Open,
    Join { ticket: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let secret_key = get_secret_key(format!("{}_secret_key.txt", args.name));

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await?;

    println!("> our endpoint id: {}", endpoint.id());
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    let ticket = get_ticket(endpoint.addr());

    let endpoint_ids = ticket.endpoints.iter().map(|p| p.id).collect();
    let (sender, receiver) = gossip.subscribe(ticket.topic, endpoint_ids).await?.split();
    println!("> joined topic!");

    let name = args.name;
    let message = Message::new(MessageBody::AboutMe {
        from: endpoint.id(),
        name: name.clone(),
    });
    sender.broadcast(message.to_vec().into()).await?;

    tokio::spawn(subscribe_loop(
        endpoint.clone(),
        name.clone(),
        sender.clone(),
        receiver,
    ));

    let (line_tx, mut line_rx) = mpsc::channel(1);
    thread::spawn(|| input_loop(line_tx));

    println!("> type a message and hit enter to broadcast...");
    while let Some(text) = line_rx.recv().await {
        let message = Message::new(MessageBody::Message {
            from: endpoint.id(),
            text: text.clone(),
        });
        sender.broadcast(message.to_vec().into()).await?;
        println!("> sent: {text}");
    }

    router.shutdown().await?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    body: MessageBody,
    nonce: [u8; 16],
}

#[derive(Debug, Serialize, Deserialize)]
enum MessageBody {
    AboutMe { from: EndpointId, name: String },
    Message { from: EndpointId, text: String },
    DocChange { from: EndpointId, text: String },
}

impl Message {
    pub fn new(body: MessageBody) -> Self {
        Self {
            body,
            nonce: rand::random(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
    }
}

async fn subscribe_loop(
    endpoint: Endpoint,
    name: String,
    sender: GossipSender,
    mut receiver: GossipReceiver,
) -> Result<()> {
    let mut names = HashMap::new();
    // iterate over all events
    while let Some(event) = receiver.try_next().await? {
        match event {
            Event::Received(msg) => {
                // deserialize the message and match on the
                // message type:
                match Message::from_bytes(&msg.content)?.body {
                    MessageBody::AboutMe { from, name } => {
                        // if it's an `AboutMe` message
                        // add an entry into the map
                        // and print the name
                        names.insert(from, name.clone());
                        println!("> {} is now known as {}", from.fmt_short(), name);
                    }
                    MessageBody::Message { from, text } => {
                        // if it's a `Message` message,
                        // get the name from the map
                        // and print the message
                        let name = names
                            .get(&from)
                            .map_or_else(|| from.fmt_short().to_string(), String::to_string);
                        println!("{}: {}", name, text);
                    }
                    _ => unreachable!(),
                }
            }
            Event::Lagged => {}
            _ => {}
        }
    }
    Ok(())
}

fn input_loop(line_tx: mpsc::Sender<String>) -> Result<()> {
    let mut buffer = String::new();
    let stdin = std::io::stdin();
    loop {
        stdin.read_line(&mut buffer)?;
        line_tx.blocking_send(buffer.clone())?;
        buffer.clear();
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Ticket {
    topic: TopicId,
    endpoints: Vec<EndpointAddr>,
}

impl Ticket {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
    }

    fn from_file<P: AsRef<Path>>(path: P) -> Option<Ticket> {
        if let Ok(bytes) = fs::read(path) {
            return Some(Ticket::from_bytes(&bytes).unwrap());
        }

        None
    }

    fn to_file<P: AsRef<Path>>(&self, path: P) {
        fs::write(path, self.to_bytes()).unwrap();
    }
}

fn get_ticket(me: EndpointAddr) -> Ticket {
    const PATH: &'static str = "ticket.bin";

    let Some(mut ticket) = Ticket::from_file(PATH) else {
        let topic = TopicId::from_bytes(rand::random());
        let endpoints = vec![me];
        let ticket = Ticket { topic, endpoints };

        ticket.to_file(PATH);
        return ticket;
    };

    if !ticket.endpoints.iter().any(|addr| addr.id == me.id) {
        ticket.endpoints.push(me);
        ticket.to_file(PATH);
    }

    ticket
}

fn get_secret_key(path: impl AsRef<Path>) -> SecretKey {
    if let Ok(secret_key) = fs::read(&path) {
        let secret_key = SecretKey::from_bytes(secret_key.as_array().unwrap());
        return secret_key;
    }

    let secret_key = SecretKey::generate();
    fs::write(path, secret_key.to_bytes()).unwrap();

    secret_key
}
