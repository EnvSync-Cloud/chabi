//! Types module containing the value types used in Chabi

pub mod connection;
pub mod connection_manager;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

pub use connection::ConnectionContext;
pub use connection_manager::ConnectionManager;

/// Serializable value type that can store multiple data types with expiration time
#[derive(Clone, Serialize, Deserialize)]
pub enum SerializedValue {
    String {
        data: String,
        expires_at: Option<DateTime<Utc>>,
    },
    Hash {
        data: HashMap<String, String>,
        field_expiry: HashMap<String, DateTime<Utc>>, // For individual field expiry (HSETEX)
    },
    List {
        data: Vec<String>,
        expires_at: Option<DateTime<Utc>>,
    },
    Set {
        data: HashSet<String>,
        expires_at: Option<DateTime<Utc>>,
    },
    SortedSet {
        data: Vec<(String, f64)>, // member, score pairs
        expires_at: Option<DateTime<Utc>>,
    }
}

/// Runtime-only value type with Instant for efficient expiry checks
#[derive(Clone, Debug)]
pub enum Value {
    String {
        data: String,
        expires_at: Option<Instant>,
    },
    Hash {
        data: HashMap<String, String>,
        field_expiry: HashMap<String, Instant>, // For individual field expiry (HSETEX)
    },
    List {
        data: VecDeque<String>,
        expires_at: Option<Instant>,
    },
    Set {
        data: HashSet<String>,
        expires_at: Option<Instant>,
    },
    SortedSet {
        data: Vec<(String, f64)>, // member, score pairs
        expires_at: Option<Instant>,
    }
}

// Convert between Value and SerializedValue for storage
impl From<&Value> for SerializedValue {
    fn from(value: &Value) -> Self {
        // Helper function to convert Instant to DateTime<Utc>
        fn instant_to_datetime(instant: &Instant) -> DateTime<Utc> {
            let now = Instant::now();
            let remaining = if *instant > now {
                instant.duration_since(now)
            } else {
                Duration::from_secs(0)
            };
            chrono::Utc::now() + chrono::Duration::from_std(remaining).unwrap_or_default()
        }
        
        match value {
            Value::String { data, expires_at } => SerializedValue::String { 
                data: data.clone(), 
                expires_at: expires_at.as_ref().map(instant_to_datetime)
            },
            Value::Hash { data, field_expiry } => {
                let expiry_map = field_expiry.iter()
                    .map(|(k, instant)| (k.clone(), instant_to_datetime(instant)))
                    .collect();
                
                SerializedValue::Hash { 
                    data: data.clone(), 
                    field_expiry: expiry_map 
                }
            },
            Value::List { data, expires_at } => SerializedValue::List {
                data: data.iter().cloned().collect(),
                expires_at: expires_at.as_ref().map(instant_to_datetime)
            },
            Value::Set { data, expires_at } => SerializedValue::Set {
                data: data.clone(),
                expires_at: expires_at.as_ref().map(instant_to_datetime)
            },
            Value::SortedSet { data, expires_at } => SerializedValue::SortedSet {
                data: data.clone(),
                expires_at: expires_at.as_ref().map(instant_to_datetime)
            },
        }
    }
}

// Convert from SerializedValue to Value for runtime use
impl From<&SerializedValue> for Value {
    fn from(value: &SerializedValue) -> Self {
        // Helper function to convert DateTime<Utc> to Instant
        fn datetime_to_instant(dt: &DateTime<Utc>) -> Instant {
            let now = chrono::Utc::now();
            let remaining = dt.signed_duration_since(now);
            if remaining.num_milliseconds() > 0 {
                Instant::now() + Duration::from_millis(remaining.num_milliseconds() as u64)
            } else {
                // Already expired
                Instant::now()
            }
        }
        
        match value {
            SerializedValue::String { data, expires_at } => Value::String { 
                data: data.clone(), 
                expires_at: expires_at.as_ref().map(datetime_to_instant)
            },
            SerializedValue::Hash { data, field_expiry } => {
                let expiry_map = field_expiry.iter()
                    .map(|(k, dt)| (k.clone(), datetime_to_instant(dt)))
                    .collect();
                
                Value::Hash { 
                    data: data.clone(), 
                    field_expiry: expiry_map 
                }
            },
            SerializedValue::List { data, expires_at } => {
                let mut deque = VecDeque::with_capacity(data.len());
                for item in data {
                    deque.push_back(item.clone());
                }
                
                Value::List {
                    data: deque,
                    expires_at: expires_at.as_ref().map(datetime_to_instant)
                }
            },
            SerializedValue::Set { data, expires_at } => Value::Set {
                data: data.clone(),
                expires_at: expires_at.as_ref().map(datetime_to_instant)
            },
            SerializedValue::SortedSet { data, expires_at } => Value::SortedSet {
                data: data.clone(),
                expires_at: expires_at.as_ref().map(datetime_to_instant)
            },
        }
    }
}
