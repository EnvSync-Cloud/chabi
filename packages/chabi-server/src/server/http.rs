use chabi_core::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct HttpServer {}

impl HttpServer {
    pub fn new() -> Self {
        HttpServer {}
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let mut buffer = [0; 1024];

        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }

        let response =
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\": \"ok\"}";
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;

        Ok(())
    }

    pub async fn run_server(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("HTTP server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New HTTP connection from {}", addr);

            let server = self.clone();
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(socket).await {
                    eprintln!("Error handling HTTP connection from {}: {}", addr, e);
                }
            });
        }
    }
}

impl Clone for HttpServer {
    fn clone(&self) -> Self {
        HttpServer {}
    }
}
