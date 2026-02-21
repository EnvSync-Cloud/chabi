//! RESP (Redis Serialization Protocol) implementation

use bytes::{Buf, BufMut, BytesMut};
use std::fmt::{self, Display, Formatter};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Error, Debug)]
pub enum RespError {
    #[error("Invalid RESP data format")]
    InvalidFormat,
    #[error("Incomplete RESP data")]
    Incomplete,
    #[error("Integer parse error")]
    IntegerParseError,
    #[error("UTF-8 encoding error")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("I/O error")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn parse(input: &[u8]) -> Result<RespValue, RespError> {
        let mut buf = BytesMut::from(input);
        let mut parser = RespParser::new();
        match parser.decode(&mut buf)? {
            Some(value) => Ok(value),
            None => Err(RespError::Incomplete),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        format!("{}", self).into_bytes()
    }
}

impl Display for RespValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RespValue::SimpleString(s) => write!(f, "+{}\r\n", s),
            RespValue::Error(msg) => write!(f, "-{}\r\n", msg),
            RespValue::Integer(n) => write!(f, ":{}\r\n", n),
            RespValue::BulkString(data) => match data {
                Some(bytes) => {
                    let s = String::from_utf8_lossy(bytes);
                    write!(f, "${}\r\n{}\r\n", bytes.len(), s)
                }
                None => write!(f, "$-1\r\n"),
            },
            RespValue::Array(items) => match items {
                Some(array) => {
                    write!(f, "*{}\r\n", array.len())?;
                    for item in array {
                        write!(f, "{}", item)?;
                    }
                    Ok(())
                }
                None => write!(f, "*-1\r\n"),
            },
        }
    }
}

#[derive(Default)]
pub struct RespParser;

impl RespParser {
    pub fn new() -> Self {
        RespParser
    }

    fn read_line(&self, buf: &mut BytesMut) -> Option<String> {
        if let Some(n) = buf.iter().position(|b| *b == b'\r') {
            if buf.len() <= n + 1 {
                return None;
            }
            if buf[n + 1] != b'\n' {
                return None;
            }

            let line = String::from_utf8_lossy(&buf[1..n]).to_string();
            buf.advance(n + 2);
            Some(line)
        } else {
            None
        }
    }
}

impl Decoder for RespParser {
    type Item = RespValue;
    type Error = RespError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        match buf[0] as char {
            '+' => self.parse_simple_string(buf),
            '-' => self.parse_error(buf),
            ':' => self.parse_integer(buf),
            '$' => self.parse_bulk_string(buf),
            '*' => self.parse_array(buf),
            _ => Err(RespError::InvalidFormat),
        }
    }
}

impl Encoder<RespValue> for RespParser {
    type Error = RespError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => {
                dst.put_u8(b'+');
                dst.put_slice(s.as_bytes());
                dst.put_slice(b"\r\n");
            }
            RespValue::Error(msg) => {
                dst.put_u8(b'-');
                dst.put_slice(msg.as_bytes());
                dst.put_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                dst.put_u8(b':');
                dst.put_slice(n.to_string().as_bytes());
                dst.put_slice(b"\r\n");
            }
            RespValue::BulkString(data) => {
                dst.put_u8(b'$');
                match data {
                    Some(bytes) => {
                        dst.put_slice(bytes.len().to_string().as_bytes());
                        dst.put_slice(b"\r\n");
                        dst.put_slice(&bytes);
                        dst.put_slice(b"\r\n");
                    }
                    None => {
                        dst.put_slice(b"-1\r\n");
                    }
                }
            }
            RespValue::Array(items) => {
                dst.put_u8(b'*');
                match items {
                    Some(array) => {
                        dst.put_slice(array.len().to_string().as_bytes());
                        dst.put_slice(b"\r\n");
                        for item in array {
                            self.encode(item, dst)?;
                        }
                    }
                    None => {
                        dst.put_slice(b"-1\r\n");
                    }
                }
            }
        }
        Ok(())
    }
}

impl RespParser {
    fn parse_simple_string(&self, buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(line) = self.read_line(buf) {
            Ok(Some(RespValue::SimpleString(line)))
        } else {
            Ok(None)
        }
    }

    fn parse_error(&self, buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(line) = self.read_line(buf) {
            Ok(Some(RespValue::Error(line)))
        } else {
            Ok(None)
        }
    }

    fn parse_integer(&self, buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(line) = self.read_line(buf) {
            match line.parse::<i64>() {
                Ok(n) => Ok(Some(RespValue::Integer(n))),
                Err(_) => Err(RespError::IntegerParseError),
            }
        } else {
            Ok(None)
        }
    }

    fn parse_bulk_string(&self, buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(len_str) = self.read_line(buf) {
            match len_str.parse::<isize>() {
                Ok(-1) => Ok(Some(RespValue::BulkString(None))),
                Ok(len) if len >= 0 => {
                    let len = len as usize;
                    if buf.len() < len + 2 {
                        return Ok(None);
                    }
                    let data = buf.split_to(len + 2);
                    Ok(Some(RespValue::BulkString(Some(data[..len].to_vec()))))
                }
                _ => Err(RespError::InvalidFormat),
            }
        } else {
            Ok(None)
        }
    }

    fn parse_array(&mut self, buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if let Some(len_str) = self.read_line(buf) {
            match len_str.parse::<isize>() {
                Ok(-1) => Ok(Some(RespValue::Array(None))),
                Ok(len) if len >= 0 => {
                    let len = len as usize;
                    let mut items = Vec::with_capacity(len);
                    for _ in 0..len {
                        match self.decode(buf)? {
                            Some(item) => items.push(item),
                            None => return Ok(None),
                        }
                    }
                    Ok(Some(RespValue::Array(Some(items))))
                }
                _ => Err(RespError::InvalidFormat),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_parse_simple_string() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("+OK\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::SimpleString("OK".to_string())));
    }

    #[test]
    fn test_parse_error() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("-Error message\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::Error("Error message".to_string())));
    }

    #[test]
    fn test_parse_integer() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from(":1000\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::Integer(1000)));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("$5\r\nhello\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::BulkString(Some(b"hello".to_vec()))));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("$-1\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::BulkString(None)));
    }

    #[test]
    fn test_parse_array() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"hello".to_vec())),
                RespValue::BulkString(Some(b"world".to_vec())),
            ])))
        );
    }

    #[test]
    fn test_parse_null_array() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("*-1\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::Array(None)));
    }

    #[test]
    fn test_encode_simple_string() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser
            .encode(RespValue::SimpleString("OK".to_string()), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser
            .encode(RespValue::Error("ERR something".to_string()), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], b"-ERR something\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser.encode(RespValue::Integer(42), &mut buf).unwrap();
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser
            .encode(RespValue::BulkString(Some(b"hello".to_vec())), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser
            .encode(RespValue::BulkString(None), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser
            .encode(
                RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(2)])),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf[..], b"*2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn test_encode_null_array() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser.encode(RespValue::Array(None), &mut buf).unwrap();
        assert_eq!(&buf[..], b"*-1\r\n");
    }

    #[test]
    fn test_serialize_display() {
        let val = RespValue::SimpleString("OK".to_string());
        assert_eq!(val.serialize(), b"+OK\r\n");
        let val = RespValue::Integer(100);
        assert_eq!(val.serialize(), b":100\r\n");
    }

    #[test]
    fn test_parse_static_method() {
        let val = RespValue::parse(b"+OK\r\n").unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_incomplete() {
        let result = RespValue::parse(b"+OK");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_invalid_format() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from("!invalid\r\n".as_bytes());
        let result = parser.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_negative_integer() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from(":-100\r\n".as_bytes());
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::Integer(-100)));
    }

    #[test]
    fn test_encode_nested_array() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::new();
        parser
            .encode(
                RespValue::Array(Some(vec![
                    RespValue::Array(Some(vec![RespValue::Integer(1)])),
                    RespValue::BulkString(Some(b"hi".to_vec())),
                ])),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf[..], b"*2\r\n*1\r\n:1\r\n$2\r\nhi\r\n");
    }
}
