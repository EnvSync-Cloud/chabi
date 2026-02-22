//! Connection commands implementation

use super::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use async_trait::async_trait;

fn extract_string(val: &RespValue) -> Option<String> {
    match val {
        RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

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

/// Handler for SELECT command
pub struct SelectCommand;

impl SelectCommand {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl CommandHandler for SelectCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        let index = match args.first() {
            Some(RespValue::BulkString(Some(bytes))) => String::from_utf8_lossy(bytes).to_string(),
            _ => {
                return Ok(RespValue::Error(
                    "ERR wrong number of arguments for 'select' command".to_string(),
                ));
            }
        };
        match index.parse::<u32>() {
            Ok(0..=15) => Ok(RespValue::SimpleString("OK".to_string())),
            Ok(_) => Ok(RespValue::Error("ERR DB index is out of range".to_string())),
            Err(_) => Ok(RespValue::Error(
                "ERR value is not an integer or out of range".to_string(),
            )),
        }
    }
}

impl Default for SelectCommand {
    fn default() -> Self {
        Self::new()
    }
}

/// Handler for QUIT command
pub struct QuitCommand;

impl QuitCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for QuitCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for QuitCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

/// Handler for RESET command
pub struct ResetCommand;

impl ResetCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ResetCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ResetCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        Ok(RespValue::SimpleString("RESET".to_string()))
    }
}

/// Handler for AUTH command (no password configured - always OK)
pub struct AuthCommand;

impl AuthCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for AuthCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for AuthCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

/// Handler for CLIENT command (stub with SETNAME, GETNAME, ID, LIST)
pub struct ClientCommand;

impl ClientCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ClientCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ClientCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'client' command".to_string(),
            ));
        }
        let subcmd = match extract_string(&args[0]) {
            Some(s) => s.to_uppercase(),
            None => return Ok(RespValue::Error("ERR invalid subcommand".to_string())),
        };
        match subcmd.as_str() {
            "SETNAME" => Ok(RespValue::SimpleString("OK".to_string())),
            "GETNAME" => Ok(RespValue::BulkString(None)),
            "ID" => Ok(RespValue::Integer(1)),
            "LIST" => Ok(RespValue::BulkString(Some(
                b"id=1 fd=0 name= db=0".to_vec(),
            ))),
            "INFO" => Ok(RespValue::BulkString(Some(
                b"id=1 fd=0 name= db=0".to_vec(),
            ))),
            _ => Ok(RespValue::Error(format!(
                "ERR unknown subcommand '{}'",
                subcmd
            ))),
        }
    }
}

/// Handler for HELLO command
pub struct HelloCommand;

impl HelloCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HelloCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for HelloCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        // Return server info as array of key-value pairs
        Ok(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"server".to_vec())),
            RespValue::BulkString(Some(b"chabi".to_vec())),
            RespValue::BulkString(Some(b"version".to_vec())),
            RespValue::BulkString(Some(b"0.1.0".to_vec())),
            RespValue::BulkString(Some(b"proto".to_vec())),
            RespValue::Integer(2),
            RespValue::BulkString(Some(b"mode".to_vec())),
            RespValue::BulkString(Some(b"standalone".to_vec())),
        ])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping_command() {
        let cmd = PingCommand::new();
        let result = cmd.execute(vec![]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));

        let message = RespValue::BulkString(Some("Hello".as_bytes().to_vec()));
        let result = cmd.execute(vec![message.clone()]).await.unwrap();
        assert_eq!(result, message);
    }

    #[tokio::test]
    async fn test_echo_command() {
        let cmd = EchoCommand::new();
        let result = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(result, RespValue::Error(_)));

        let message = RespValue::BulkString(Some("Hello".as_bytes().to_vec()));
        let result = cmd.execute(vec![message.clone()]).await.unwrap();
        assert_eq!(result, message);
    }

    #[tokio::test]
    async fn test_select_command() {
        let cmd = SelectCommand::new();
        let result = cmd
            .execute(vec![RespValue::BulkString(Some(b"0".to_vec()))])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = cmd
            .execute(vec![RespValue::BulkString(Some(b"1".to_vec()))])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = cmd
            .execute(vec![RespValue::BulkString(Some(b"15".to_vec()))])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = cmd
            .execute(vec![RespValue::BulkString(Some(b"16".to_vec()))])
            .await
            .unwrap();
        assert!(matches!(result, RespValue::Error(_)));
    }

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn test_quit() {
        let cmd = QuitCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_reset() {
        let cmd = ResetCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("RESET".to_string()));
    }

    #[tokio::test]
    async fn test_auth() {
        let cmd = AuthCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_setname() {
        let cmd = ClientCommand::new();
        let r = cmd
            .execute(vec![bulk("SETNAME"), bulk("myconn")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_getname() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![bulk("GETNAME")]).await.unwrap();
        assert_eq!(r, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_client_id() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![bulk("ID")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_client_unknown_subcmd() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![bulk("FOOBAR")]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_client_wrong_args() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_hello() {
        let cmd = HelloCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                assert!(arr.len() >= 4); // Has server, version, proto, mode pairs
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_select_invalid() {
        let cmd = SelectCommand::new();
        let r = cmd.execute(vec![bulk("abc")]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_ping_with_message() {
        let cmd = PingCommand::new();
        let msg = bulk("hello world");
        let r = cmd.execute(vec![msg.clone()]).await.unwrap();
        assert_eq!(r, msg);
    }

    #[tokio::test]
    async fn test_client_list() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![bulk("LIST")]).await.unwrap();
        match r {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                assert!(s.contains("id=1"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[tokio::test]
    async fn test_client_info() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![bulk("INFO")]).await.unwrap();
        match r {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                assert!(s.contains("id=1"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[tokio::test]
    async fn test_select_no_args() {
        let cmd = SelectCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_select_non_bulkstring_arg() {
        let cmd = SelectCommand::new();
        let r = cmd.execute(vec![RespValue::Integer(0)]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_client_non_bulkstring_subcmd() {
        let cmd = ClientCommand::new();
        let r = cmd.execute(vec![RespValue::Integer(1)]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_ping_default() {
        let cmd = PingCommand::default();
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("PONG".to_string()));
    }

    #[tokio::test]
    async fn test_echo_default() {
        let cmd = EchoCommand::default();
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }
}
