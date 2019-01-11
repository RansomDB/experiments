#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate futures;
extern crate bincode;
extern crate bytes;
extern crate tokio;
extern crate tokio_serde_bincode;

use bincode::{serialize, deserialize};
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::codec::{FramedRead, LengthDelimitedCodec, length_delimited};
use futures::sync::mpsc;
use futures::future::{self, Either};
use bytes::{Bytes, BytesMut, BufMut};
use tokio_serde_bincode::{ReadBincode, WriteBincode};

use std::collections::HashMap;
use std::net::{SocketAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::env;

type Tx = mpsc::UnboundedSender<Bytes>;
type Rx = mpsc::UnboundedReceiver<Bytes>;

struct Cluster {
    peers_tx: HashMap<SocketAddr, Tx>,
    clock: u64,
}

impl Cluster {
    fn new() -> Self {
        Cluster {
            peers_tx: HashMap::new(),
            clock: 0
        }
    }
}

struct Peer {
    addr: SocketAddr,
    handle: String,
    cluster: Arc<Mutex<Cluster>>,
}

impl Peer {

    fn new(cluster: Arc<Mutex<Cluster>>) -> Self {
        Peer {

        }
    }
}

// How to we convenientl store the logical clock on each message without
// modifying the message structs?
#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum Message {
    JoinClusterMsg(JoinCluster),
    LeaveClusterMsg(LeaveCluster),
}

impl From<JoinCluster> for Message {
    fn from(jc: JoinCluster) -> Self {
        Message::JoinClusterMsg(jc)
    }
}

impl From<LeaveCluster> for Message {
    fn from(lc: LeaveCluster) -> Self {
        Message::LeaveClusterMsg(lc)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct JoinCluster {
    ip: String,
    port: u32,
    handle: String
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct LeaveCluster {
    ip: String,
    port: u32
}

// FramedRead upgrades TcpStream from an AsyncRead to a Stream
type IOErrorStream = FramedRead<TcpStream, LengthDelimitedCodec>;

// stream::FromErr maps underlying IO errors into Bincode errors
type BincodeErrStream = stream::FromErr<IOErrorStream, bincode::Error>;

// ReadBincode maps underlying bytes into Bincode-deserializable structs
type BincodeStream = ReadBincode<BincodeErrStream, Message>;


fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} <port>", args[0]);
        return;
    }
    let cluster_state = Arc::new(Mutex::new(Cluster::new()));

    let ip_addr: Ipv4Addr = "0.0.0.0".parse().unwrap();
    let port: u16 = args[1].parse().map_err(|_| "could not parse port").unwrap();
    let addr = SocketAddr::from((ip_addr, port));

    let listener = TcpListener::bind(&addr).map_err(|_| "failed to bind").unwrap();
    println!("Listening on: {}", addr);

    let server = listener.incoming()
        .map_err(|e| println!("error accepting socket; error = {:?}", e))
        .for_each(move |socket| {
            println!("Client connected");
            let delimited_stream: BincodeErrStream = length_delimited::Builder::new()
                .new_read(socket)
                .from_err::<bincode::Error>();

            let deserialized: BincodeStream = ReadBincode::new(delimited_stream);

            tokio::spawn(
                deserialized
                    .for_each(|msg| Ok(println!("GOT: {:?}", msg)))
                    .map_err(|_| ()),
            );

            Ok(())
        });

    tokio::run(server);

    // let msg1: Message = JoinCluster {
    //     ip: String::from("127.0.0.1"),
    //     port: 3400,
    //     handle: String::from("node1")
    // }.into();

    // let msg2: Message = LeaveCluster {
    //     ip: String::from("127.0.0.1"),
    //     port: 3400,
    // }.into();

    // println!("Message: {:?}", msg1);

    // let encoded1 = serialize(&msg1).unwrap();
    // let encoded2 = serialize(&msg2).unwrap();
    // // println!("Encoded data2 is {} bytes", encoded2.len());

    // for e in vec!(encoded1, encoded2) {
    //     match deserialize(&e[..]).unwrap() {
    //         Message::JoinClusterMsg(decoded) => {
    //             println!("Decoded JoinCluster message: {:?}", decoded);
    //         },
    //         Message::LeaveClusterMsg(decoded) => {
    //             println!("Decoded LeaveCluster message: {:?}", decoded);
    //         },
    //     }
    // }
}