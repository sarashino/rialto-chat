extern crate alphanumeric_sort;

use std::collections::LinkedList;
use std::vec::Vec;

use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;

use postgres::stmt::{Statement};
use postgres::rows::{Row, Rows};
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use r2d2;
use r2d2::{Pool,PooledConnection};

use super::types::{Message, MessageRow, mkMessage};

const FMT: &str = &"%Y-%m-%dT%H:%M:%S.%f";

fn sort_ids<'S>(fst_user_id: &'S str, snd_user_id: &'S str) -> (&'S str, &'S str) {
    let mut sortids = [fst_user_id, snd_user_id];
    sortids.sort();
    let [f,s] = sortids;
    (f, s)
}

pub fn selectAllMessagesByRoom(conn: PooledConnection<PostgresConnectionManager>, room: &str) -> Vec<Message>{
    let prepared = conn.prepare("SELECT room, first_user_id, second_user_id, from_first, message, TO_CHAR(timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS.US'), TO_CHAR(update_timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM chat.chat WHERE room = $1 ORDER BY timestamp ASC;").unwrap();

    rows_to_messages(prepared.query(&[&room]).expect("select all messages by room failed"))
}
pub fn selectAllMessagesByUserIds(conn: PooledConnection<PostgresConnectionManager>, first_user_id: &str, second_user_id: &str) -> Vec<Message>{
    let (f,s) = sort_ids(first_user_id, second_user_id);
    let prepared = conn.prepare("SELECT room, first_user_id, second_user_id, from_first, message, TO_CHAR(timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS.US'), TO_CHAR(update_timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') FROM chat.chat WHERE first_user_id = $1 AND second_user_id = $2 ORDER BY timestamp ASC;").unwrap();

    rows_to_messages(prepared.query(&[&f,&s]).expect("select all messages by user id failed"))
}
fn rows_to_messages(rows: Rows) -> Vec<Message> {
    fn rowToMessage(row: Row) -> MessageRow {
        let timestamp: String = row.get(5);
        let timestamp = NaiveDateTime::parse_from_str(&timestamp, FMT).expect("parse naivedatetime from str");
        let update_timestamp_str: Option<String> = row.get(6);
        let mut update_timestamp: Option<NaiveDateTime> = None;
        if let Some(update_timestamp_str) = update_timestamp_str {
            update_timestamp = Some(NaiveDateTime::parse_from_str(&update_timestamp_str, FMT).expect("parse naivedatetime from str update"));
        }
        MessageRow {
                room: row.get(0),
                first_user_id: row.get(1),
                second_user_id: row.get(2),
                from_first: row.get(3),
                message: row.get(4),
                timestamp: timestamp,
                update_timestamp: update_timestamp
        }
    };
    let mut list: Vec<Message> = Vec::new();
    for row in rows.into_iter() {
        list.push(mkMessage(rowToMessage(row)));
    }
    return list;
}


pub fn insert_message_stmt<'s>(conn: &'s PooledConnection<PostgresConnectionManager>, now: &'s str) -> Statement<'s> {
    conn.prepare(&format!("INSERT INTO chat.chat (room, first_user_id, second_user_id, from_first, message, timestamp, update_timestamp) VALUES ($1, $2, $3, $4, $5, '{}', NULL);", now)).unwrap()
}
pub fn insert_message(conn: PooledConnection<PostgresConnectionManager>, sender_id: &str, receiver_id: &str, room: &Option<String>, message: &str) {
    let (f,s) = sort_ids(sender_id, receiver_id);
    let from_first = (&sender_id, &receiver_id) == (&f, &s);
    let now = Utc::now();
    let now = NaiveDateTime::from_timestamp(now.timestamp(), now.timestamp_subsec_micros());
    let now = now.format(FMT).to_string();
    let prepared = insert_message_stmt(&conn, &now);
    prepared.execute(&[&room, &f, &s, &from_first, &message]).unwrap();
}

//pub fn update_message_stmt(conn: PooledConnection<PostgresConnectionManager>) -> Statement {
//    conn.prepare("UPDATE chat.chat SET update_timestamp = $1, message = $2 WHERE first_user_id = $3 AND second_user_id = $4 AND from_first = $5 AND timestamp = $6;")
//}

// Room

pub fn enterIntoRoomAndCreateIfNotFound(conn: PooledConnection<PostgresConnectionManager>, room: &str, sender_id: &str) -> PooledConnection<PostgresConnectionManager> {
    let r = conn.query("SELECT COUNT(1)::INTEGER FROM chat.room_master WHERE room=$1", &[&room]);
    if let Ok(r) = r {
        let r: i32 = r.get(0).get(0);
        if r == 0 {
            let _ = conn.query("INSERT INTO chat.room_master (room) VALUES($1)", &[&room]);
        }
    };
    let r = conn.query("SELECT COUNT(1)::INTEGER FROM chat.room WHERE room=$1 AND member_user_id=$2", &[&room, &sender_id]);
    if let Ok(r) = r {
        let r: i32 = r.get(0).get(0);
        if r == 0 {
            let _ = conn.query("INSERT INTO chat.room (room, member_user_id) VALUES($1, $2)", &[&room, &sender_id]);
        }
    };

    conn
}
pub fn exitFromRoomAndDeleteItIfEmpty(conn: PooledConnection<PostgresConnectionManager>, room: &str, sender_id: &str) {
    let _ = conn.query("DELETE FROM chat.room WHERE room=$1 AND member_user_id=$2", &[&room, &sender_id]);
    let r = conn.query("SELECT COUNT(1)::INTEGER FROM chat.room WHERE room=$1", &[&room]);
    if let Ok(r) = r {
        let r: i32 = r.get(0).get(0);
        if r == 0 {
            if let Err(e) = conn.query("DELETE FROM chat.room_master WHERE room = $1", &[&room]){
                println!("{}", e);
            }
        }
    }else if let Err(e) = r {
        println!("{}", e);
    };
}
