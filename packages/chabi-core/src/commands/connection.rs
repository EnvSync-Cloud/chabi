//! Connection commands implementation

use super::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use async_trait::async_trait;

/// Handler for PING command
pub struct PingCommand;

impl PingCommand {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl CommandHandler for PingCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        match args.first() {
            Some(message) => Ok(message.clone()),
            None => Ok(RespValue::SimpleString("PONG".to_string())),
        }
    }
}

impl Default for PingCommand {
    fn default() -> Self {
        Self::new()
    }
}

/// Handler for ECHO command
pub struct EchoCommand;

impl EchoCommand {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl CommandHandler for EchoCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        match args.first() {
            Some(message) => Ok(message.clone()),
            None => Ok(RespValue::Error(
                "ERR wrong number of arguments for 'echo' command".to_string(),
            )),
        }
    }
}

impl Default for EchoCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping_command() {
        let cmd = PingCommand::new();

        // Test PING without argument
        let result = cmd.execute(vec![]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));

        // Test PING with argument
        let message = RespValue::BulkString(Some("Hello".as_bytes().to_vec()));
        let result = cmd.execute(vec![message.clone()]).await.unwrap();
        assert_eq!(result, message);
    }

    #[tokio::test]
    async fn test_echo_command() {
        let cmd = EchoCommand::new();

        // Test ECHO without argument
        let result = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(result, RespValue::Error(_)));

        // Test ECHO with argument
        let message = RespValue::BulkString(Some("Hello".as_bytes().to_vec()));
        let result = cmd.execute(vec![message.clone()]).await.unwrap();
        assert_eq!(result, message);
    }
}
