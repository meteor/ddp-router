use anyhow::Error;
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string, Map, Value};
use tokio_tungstenite::tungstenite::Message;

/// <https://github.com/meteor/meteor/blob/devel/packages/ddp/DDP.md>
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "msg")]
pub enum DDPMessage {
    // Establishing connection.
    #[serde(rename = "connect")]
    Connect {
        #[serde(skip_serializing_if = "Option::is_none")]
        session: Option<String>,
        version: String,
        support: Vec<String>,
    },
    #[serde(rename = "connected")]
    Connected { session: String },
    #[serde(rename = "failed")]
    Failed { version: String },

    // Hearbeats.
    #[serde(rename = "ping")]
    Ping {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
    #[serde(rename = "pong")]
    Pong {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    // Managing data.
    #[serde(rename = "added")]
    Added {
        collection: String,
        id: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        fields: Option<Map<String, Value>>,
    },
    #[serde(rename = "addedBefore")]
    AddedBefore {
        collection: String,
        id: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        fields: Option<Map<String, Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        before: Option<String>,
    },
    #[serde(rename = "changed")]
    Changed {
        collection: String,
        id: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        fields: Option<Map<String, Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cleared: Option<Vec<String>>,
    },
    #[serde(rename = "movedBefore")]
    MovedBefore {
        collection: String,
        id: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        before: Option<String>,
    },
    #[serde(rename = "removed")]
    Removed { collection: String, id: Value },

    // Managing subscriptions.
    #[serde(rename = "nosub")]
    Nosub {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<Value>,
    },
    #[serde(rename = "ready")]
    Ready { subs: Vec<String> },
    #[serde(rename = "sub")]
    Sub {
        id: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        params: Option<Vec<Value>>,
    },
    #[serde(rename = "unsub")]
    Unsub { id: String },

    // Remote procedure calls.
    #[serde(rename = "method")]
    Method {
        id: String,
        method: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        params: Option<Vec<Value>>,
        #[serde(rename = "randomSeed", skip_serializing_if = "Option::is_none")]
        random_seed: Option<String>,
    },
    #[serde(rename = "result")]
    Result {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
    },
    #[serde(rename = "updated")]
    Updated { methods: Vec<String> },
}

impl TryFrom<DDPMessage> for Message {
    type Error = Error;
    fn try_from(ddp_message: DDPMessage) -> Result<Self, Self::Error> {
        Ok(Self::text(to_string(&ddp_message)?))
    }
}

impl TryFrom<Message> for DDPMessage {
    type Error = Error;
    fn try_from(raw_message: Message) -> Result<Self, Self::Error> {
        Ok(from_str(raw_message.to_text()?)?)
    }
}
