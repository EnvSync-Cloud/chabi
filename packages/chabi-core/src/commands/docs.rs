//! Documentation commands implementation

use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// Command to get documentation for Redis commands
#[derive(Clone)]
pub struct DocsCommand {}

impl Default for DocsCommand {
    fn default() -> Self {
        Self::new()
    }
}

impl DocsCommand {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl CommandHandler for DocsCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        let mut docs = HashMap::new();

        // String commands
        docs.insert("GET", "Get the value of a key");
        docs.insert("SET", "Set the string value of a key");

        // Hash commands
        docs.insert("HGET", "Get the value of a hash field");
        docs.insert("HSET", "Set the string value of a hash field");
        docs.insert("HSETEX", "Set the value and expiration of a hash field");
        docs.insert("HGETALL", "Get all the fields and values in a hash");
        docs.insert("HEXISTS", "Determine if a hash field exists");
        docs.insert("HDEL", "Delete one or more hash fields");
        docs.insert("HLEN", "Get the number of fields in a hash");
        docs.insert("HKEYS", "Get all the fields in a hash");
        docs.insert("HVALS", "Get all the values in a hash");

        // List commands
        docs.insert("LPUSH", "Prepend one or multiple values to a list");
        docs.insert("RPUSH", "Append one or multiple values to a list");
        docs.insert("LPOP", "Remove and get the first element in a list");
        docs.insert("RPOP", "Remove and get the last element in a list");
        docs.insert("LRANGE", "Get a range of elements from a list");
        docs.insert("LLEN", "Get the length of a list");

        // Set commands
        docs.insert("SADD", "Add one or more members to a set");
        docs.insert("SMEMBERS", "Get all the members in a set");
        docs.insert(
            "SISMEMBER",
            "Determine if a given value is a member of a set",
        );
        docs.insert("SCARD", "Get the number of members in a set");
        docs.insert("SREM", "Remove one or more members from a set");

        // Key commands
        docs.insert("DEL", "Delete a key");
        docs.insert("EXISTS", "Determine if a key exists");
        docs.insert("KEYS", "Find all keys matching the given pattern");
        docs.insert("RENAME", "Rename a key");
        docs.insert("TYPE", "Determine the type stored at key");

        // PubSub commands
        docs.insert("PUBLISH", "Post a message to a channel");
        docs.insert(
            "SUBSCRIBE",
            "Listen for messages published to the given channels",
        );
        docs.insert(
            "UNSUBSCRIBE",
            "Stop listening for messages posted to the given channels",
        );
        docs.insert("PUBSUB", "Inspect the state of the Pub/Sub subsystem");

        // Server commands
        docs.insert("INFO", "Get information and statistics about the server");
        docs.insert("SAVE", "Synchronously save the dataset to disk");

        // Documentation commands
        docs.insert("DOCS", "Get documentation for Redis commands");
        docs.insert("COMMAND", "Get array of Redis command details");

        let mut result = Vec::new();
        for (command, description) in docs {
            result.push(RespValue::BulkString(Some(command.as_bytes().to_vec())));
            result.push(RespValue::BulkString(Some(description.as_bytes().to_vec())));
        }

        Ok(RespValue::Array(Some(result)))
    }
}

#[cfg(test)]
mod docs_tests {
    use super::*;

    #[tokio::test]
    async fn test_docs_command() {
        let cmd = DocsCommand::new();
        let result = cmd.execute(vec![]).await.unwrap();
        match result {
            RespValue::Array(Some(arr)) => {
                assert!(!arr.is_empty());
                // Should be pairs of (command, description)
                assert_eq!(arr.len() % 2, 0);
            }
            _ => panic!("Expected non-empty Array"),
        }
    }

    #[tokio::test]
    async fn test_docs_default() {
        let cmd = DocsCommand::default();
        let result = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(result, RespValue::Array(Some(_))));
    }
}

/// Command to get Redis command details
#[derive(Clone)]
pub struct CommandCommand {}

impl CommandCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for CommandCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for CommandCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        let mut commands = HashMap::new();

        // String commands
        commands.insert("GET", vec!["readonly", "string"]);
        commands.insert("SET", vec!["write", "string"]);

        // Hash commands
        commands.insert("HGET", vec!["readonly", "hash"]);
        commands.insert("HSET", vec!["write", "hash"]);
        commands.insert("HSETEX", vec!["write", "hash"]);
        commands.insert("HGETALL", vec!["readonly", "hash"]);
        commands.insert("HEXISTS", vec!["readonly", "hash"]);
        commands.insert("HDEL", vec!["write", "hash"]);
        commands.insert("HLEN", vec!["readonly", "hash"]);
        commands.insert("HKEYS", vec!["readonly", "hash"]);
        commands.insert("HVALS", vec!["readonly", "hash"]);

        // List commands
        commands.insert("LPUSH", vec!["write", "list"]);
        commands.insert("RPUSH", vec!["write", "list"]);
        commands.insert("LPOP", vec!["write", "list"]);
        commands.insert("RPOP", vec!["write", "list"]);
        commands.insert("LRANGE", vec!["readonly", "list"]);
        commands.insert("LLEN", vec!["readonly", "list"]);

        // Set commands
        commands.insert("SADD", vec!["write", "set"]);
        commands.insert("SMEMBERS", vec!["readonly", "set"]);
        commands.insert("SISMEMBER", vec!["readonly", "set"]);
        commands.insert("SCARD", vec!["readonly", "set"]);
        commands.insert("SREM", vec!["write", "set"]);

        // Key commands
        commands.insert("DEL", vec!["write", "generic"]);
        commands.insert("EXISTS", vec!["readonly", "generic"]);
        commands.insert("KEYS", vec!["readonly", "generic"]);
        commands.insert("RENAME", vec!["write", "generic"]);
        commands.insert("TYPE", vec!["readonly", "generic"]);

        // PubSub commands
        commands.insert("PUBLISH", vec!["pubsub", "publish"]);
        commands.insert("SUBSCRIBE", vec!["pubsub", "subscribe"]);
        commands.insert("UNSUBSCRIBE", vec!["pubsub", "subscribe"]);
        commands.insert("PUBSUB", vec!["pubsub", "admin"]);

        // Server commands
        commands.insert("INFO", vec!["readonly", "server"]);
        commands.insert("SAVE", vec!["admin", "server"]);

        // Documentation commands
        commands.insert("DOCS", vec!["readonly", "server"]);
        commands.insert("COMMAND", vec!["readonly", "server"]);

        let mut result = Vec::new();
        for (command, categories) in commands {
            let mut command_info = Vec::new();
            command_info.push(RespValue::BulkString(Some(command.as_bytes().to_vec())));

            let mut categories_array = Vec::new();
            for category in categories {
                categories_array.push(RespValue::BulkString(Some(category.as_bytes().to_vec())));
            }
            command_info.push(RespValue::Array(Some(categories_array)));

            result.push(RespValue::Array(Some(command_info)));
        }

        Ok(RespValue::Array(Some(result)))
    }
}

#[cfg(test)]
mod command_tests {
    use super::*;

    #[tokio::test]
    async fn test_command_command() {
        let cmd = CommandCommand::new();
        let result = cmd.execute(vec![]).await.unwrap();
        match result {
            RespValue::Array(Some(arr)) => {
                assert!(!arr.is_empty());
                // Each entry is an Array of [name, categories_array]
                for item in &arr {
                    match item {
                        RespValue::Array(Some(inner)) => {
                            assert_eq!(inner.len(), 2);
                        }
                        _ => panic!("Expected Array for command info"),
                    }
                }
            }
            _ => panic!("Expected non-empty Array"),
        }
    }

    #[tokio::test]
    async fn test_command_default() {
        let cmd = CommandCommand::default();
        let result = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(result, RespValue::Array(Some(_))));
    }
}
