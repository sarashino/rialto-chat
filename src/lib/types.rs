use std::string::ToString;

use chrono::naive::NaiveDateTime;
use chrono::naive::serde::ts_nanoseconds;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub room: Option<String>,
    pub senderId: String,
    pub receiverId: String,
    pub message: String,
    pub timestamp: NaiveDateTime,
    pub update_timestamp: Option<NaiveDateTime>
}

#[derive(Deserialize, Debug)]
pub struct MessageRow {
    pub room: Option<String>,
    pub first_user_id: String,
    pub second_user_id: String,
    pub from_first: bool,
    pub message: String,
    pub timestamp: NaiveDateTime,
    pub update_timestamp: Option<NaiveDateTime>,
}

pub fn mkMessage(row: MessageRow) -> Message{
    if row.from_first
    {
        Message {
            room: row.room,
            senderId: row.first_user_id,
            receiverId: row.second_user_id,
            message: row.message,
            timestamp: row.timestamp,
            update_timestamp: row.update_timestamp
        }
    } else {
        Message {
            room: row.room,
            senderId: row.second_user_id,
            receiverId: row.first_user_id,
            message: row.message,
            timestamp: row.timestamp,
            update_timestamp: row.update_timestamp
        }
    }

}
