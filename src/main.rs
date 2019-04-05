extern crate alphanumeric_sort;

use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::ops::DerefMut;
use crossbeam_utils::thread as cbthread;
//use crossbeam_channel::{unbounded, Receiver, Sender};

use postgres::notification::Notification;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use r2d2;
use r2d2::{Pool,PooledConnection};
use fallible_iterator::FallibleIterator;

use serde_json::{to_string, from_str};

use websocket::sync::Server;
use websocket::stream::sync::{Splittable, Stream};
use websocket::server::upgrade::sync::Upgrade;
use websocket::server::{WsServer, NoTlsAcceptor};
use std::net::{TcpListener, TcpStream};
use websocket::message::Message as wsMessage;
use websocket::{OwnedMessage};
use websocket::receiver::{Reader};
use websocket::sender::{Writer};

mod lib;
use crate::lib::types::{Message, MessageRow, mkMessage};
use crate::lib::prepared_sql::{ selectAllMessagesByUserIds
                              , selectAllMessagesByRoom
                              , insert_message
                              , exitFromRoomAndDeleteItIfEmpty
                              , enterIntoRoomAndCreateIfNotFound
                              };

type Notifier =  Arc<Mutex<Vec<(thread::ThreadId, mpsc::Sender<Message>)>>>;

fn main() {
    let pg_conn = PostgresConnectionManager::new("postgres://user_chat@localhost:5432/ensense_chat", TlsMode::None).expect("Could not connect to PostgreSQL");
    //let pg_conn = PostgresConnectionManager::new("postgres://user_chat:user_chat@db1pgs-ensense-database-test.cvcldbi6xmic.ap-northeast-1.rds.amazonaws.com:5432/ensense_chat", TlsMode::None).expect("Could not connect to PostgreSQL");
    let pool = r2d2::Pool::new(pg_conn).expect("new failed");
    let wsServer = Server::bind("192.168.2.150:8081").unwrap();
    let notifier: Notifier = Arc::new(Mutex::new(vec![]));
    let poolN = pool.clone();
    {
        let notifier = notifier.clone();
        thread::spawn(move || {
            realtimeNotify(wsServer, poolN, notifier);
        });
    }
    listen_postgres(pool.get().expect("pool get for listen failed"), "rtchat".to_string(), send_handler(notifier));
}

fn realtimeNotify(sock: WsServer<NoTlsAcceptor, TcpListener>, pool: Pool<PostgresConnectionManager> , notifier: Notifier) {
    cbthread::scope(|scope| {
    for request in sock.filter_map(Result::ok) {
        let pool = pool.clone();
        let notifier = notifier.clone();
        scope.spawn(move || {
	//		if !request.protocols().contains(&"rust-websocket".to_string()) {
	//			request.reject().unwrap();
	//			return;
	//		}
            let uri = request.uri();
            let path: Vec<&str> = uri.split("/").collect();
			if !(path.len() > 2 && !uri.contains("?")) {
				request.reject().unwrap();
				return;
			}
            let sender_id = path[1].to_lowercase();
            let receiver_id = path[2].to_lowercase();
            // must check either room or user id is valid
            // check id or room exists
            // if group chat
            let room = if sender_id == "room"
                    { Some(&receiver_id) } else { None };
            let sender_id = if sender_id == "room" { path[3].to_lowercase() } else { sender_id };

			let mut client = request.use_protocol("rust-websocket").accept().unwrap();

			let ip = client.peer_addr().unwrap();

			println!("Connection from {}", ip);

			let (mut receiver, mut sender) = client.split().unwrap();

            let messages = { if let Some(room) = &room {
                    let conn = pool.get().expect("pool get");
                    let conn = enterIntoRoomAndCreateIfNotFound(conn, &room, &sender_id);
                    selectAllMessagesByRoom(conn, &room)
                } else {
                    selectAllMessagesByUserIds(pool.get().expect("pool get"), &sender_id, &receiver_id)
                }};
            let messages = to_string(&messages).unwrap_or_else(|_| panic!("{:?}", messages));
            let messages = wsMessage::text(messages);
            sender.send_message(&messages).unwrap_or_else(|_| panic!("sending failed: {:?}", messages));

            // to shutdown receiver thread safely
            let (tx, rx) = mpsc::channel();
            let room = match room {
                Some(x) => Some(x.to_string()),
                None => None,
            };
            {
                let room = room.clone();
                let pool = pool.clone();
                // global thread
                spawn_sender(pool, receiver, tx, sender_id.to_string(), receiver_id.to_string(), room, ip.to_string());
            }
            {
                let room = room.clone();
                // scoped thread
                spawn_receiver(sender, notifier, rx, sender_id.to_string(), receiver_id.to_string(), room);
            }
            if let Some(room) = room {
                let conn = pool.get().expect("pool get");
                exitFromRoomAndDeleteItIfEmpty(conn, &room, &sender_id);
            };
		});
	}
    });
}

fn spawn_receiver(mut sender: Writer<<TcpStream as Splittable>::Writer>, notifier: Notifier, rx: mpsc::Receiver<()>, sender_id: String, receiver_id: String, room: Option<String>){
    cbthread::scope(|s|{
        let (client_tx, client_rx) = mpsc::channel();
        let threadId = thread::current().id();
        {
            notifier.lock().unwrap().push((threadId, client_tx));
        }
        s.spawn(move || {
            loop {
                if let Ok(received) = client_rx.try_recv() {
                    let mut chatid = [&sender_id, &receiver_id];
                    let mut incoming_chatid = [&received.senderId, &received.receiverId];
                    chatid.sort();
                    incoming_chatid.sort();
                    if received.room == room || chatid == incoming_chatid {
                        let message = to_string(&received).expect("push postgres to ws failed");
                        let message = wsMessage::text(message);
                        sender.send_message(&message);
                    }
                };
                if let Ok(()) = rx.try_recv() {
                    {
                        let mut notifier = notifier.lock().unwrap();
                        for idx in (0..notifier.len()) {
                            if threadId == notifier[idx].0 {
                                notifier.remove(idx);
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        });
    });
}

fn spawn_sender(pool: Pool<PostgresConnectionManager>, mut receiver: Reader<<TcpStream as Splittable>::Reader>, tx: mpsc::Sender<()>, sender_id: String, receiver_id: String, room: Option<String>, ip: String){
        thread::spawn(move || {
            for message in receiver.incoming_messages() {
                let message = message;

                match message {
                    Err(e) => {
                        println!("Client {} disconnected", ip);
                        break;
                    }
                    _ => {},
                }
                let message = message.unwrap();
    
                match message {
                    OwnedMessage::Close(_) => {
                        let message = OwnedMessage::Close(None);
                        println!("Client {} disconnected", ip);
                        break;
                    }
                    OwnedMessage::Text(msg) => {
                        if msg != "" {
                            insert_message(pool.get().expect("get pool conn failed"), &sender_id, &receiver_id, &room, &msg);
                        }
                    }
                    _ => {
                        //sender.send_message(&message).unwrap()
                        },
                }
            }
            if let Err(e) = tx.send(()) {
                panic!("while terminating receiver: {}", e);
            }
        });
}
fn send_handler(notifier: Notifier) -> impl Fn(Notification) {
    move |notification|{
        let notified : MessageRow = from_str(&notification.payload).unwrap();
        let message = mkMessage(notified);
        for (_, sender) in notifier.lock().unwrap().iter() {
            if let Err(e) = sender.send(message.clone()) {
                println!("{}: {:?}", e, sender);
            }
        }
    }
}

fn print_handler(notification: Notification) {
    let notified : MessageRow = from_str(&notification.payload).unwrap();
    let message = mkMessage(notified);
    println!("receive: {:?}", message);
}

fn listen_postgres<F>(conn: PooledConnection<PostgresConnectionManager>, channel: String, handler: F)
        where F: Fn(Notification) -> (){

    let listen_command = format!("LISTEN {}", channel);
    conn.execute(listen_command.as_str(), &[]).expect("Could not send LISTEN");

    let notifications = conn.notifications();

    let mut it = notifications.blocking_iter();

    println!("Waiting for notifications...");

    // could not use 'loop' here because it does not compile in --release mode
    // since Ok() is unreachable.
    #[allow(while_true)]
    while true {
        // it.next() -> Result<Option<Notification>>
        match it.next() {
            Ok(Some(notification)) => {
                handler(notification);
            },
            Err(err) => println!("Got err {:?}", err),
            _ => panic!("Unexpected state.")
        }
    }
}
