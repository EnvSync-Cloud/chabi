use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use chabi_core::{ChabiKV, PersistenceOptions};
use std::sync::Arc;
use std::time::Instant;
use std::net::SocketAddr;
use tokio::sync::{Semaphore, broadcast};
use tokio::time::{timeout, Duration};

// Configure logging
use env_logger;
use log::{info, error, debug, warn};

// Command line argument parsing
use clap::{App, Arg};

// Error handling
use anyhow::{Result, Context};

// HTTP server module (temporarily disabled)
// mod http;

// Constants
const MAX_CONCURRENT_CONNECTIONS: usize = 1000;
const CONNECTION_TIMEOUT_MS: u64 = 30000; // 30 seconds
const COMMAND_BUFFER_SIZE: usize = 16384;
const DEFAULT_HOST: &str = "0.0.0.0"; // Listen on all interfaces by default

async fn handle_client(
    stream: TcpStream, 
    addr: SocketAddr, 
    kv: Arc<ChabiKV>,
    debug_mode: bool
) -> Result<()> {
    // Use BufReader and BufWriter for more efficient I/O
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);
    
    // Buffer for reading commands
    let mut buf = vec![0u8; COMMAND_BUFFER_SIZE];
    
    // Client ID for PUB/SUB (using the socket address as a unique identifier)
    let client_id = addr.to_string();
    
    // Setup for PUB/SUB receivers - we'll collect them as the client subscribes
    let mut receivers: Vec<tokio::sync::broadcast::Receiver<String>> = Vec::new();
    
    // Maintain connection until client disconnects
    loop {
        // Check for any published messages on subscribed channels
        if !receivers.is_empty() {
            // Use select to wait for messages from any channel with a short timeout
            let mut has_message = false;
            
            // Process messages from all receivers
            for i in 0..receivers.len() {
                // Use try_recv to avoid blocking - this is important for responsive command handling
                match receivers[i].try_recv() {
                    Ok(msg) => {
                        has_message = true;
                        debug!("Received message '{}' on receiver {}", msg, i);
                        
                        // Create a connection context for this client
                        if let Some(context) = kv.connection_manager.get_client(&client_id) {
                            // Get the list of channels this client is subscribed to 
                            let channels = context.get_subscriptions();
                            
                            // Get the channel name that corresponds to this receiver index
                            // We need to match the channel to the receiver index
                            if i < channels.len() {
                                let channel = &channels[i];
                                
                                // Format message in Redis protocol format
                                let message_response = format!(
                                    "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    channel.len(), channel,
                                    msg.len(), msg
                                );
                                
                                // Send the message to the client
                                if let Err(e) = writer.write_all(message_response.as_bytes()).await {
                                    error!("Failed to send PUB/SUB message to {}: {}", addr, e);
                                    continue;
                                }
                                
                                if let Err(e) = writer.flush().await {
                                    error!("Failed to flush PUB/SUB message to {}: {}", addr, e);
                                    continue;
                                }
                                
                                debug!("Published message '{}' from channel '{}' to client {}", msg, channel, addr);
                            }
                        }
                    },
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                        // No messages in this channel, continue
                    },
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                        warn!("Client {} lagged behind {} messages", addr, n);
                    },
                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                        // Channel was closed, we'll need to refresh receivers on next command
                        debug!("Channel for receiver {} was closed", i);
                    }
                }
            }
            
            // If we found messages, give a small delay to batch more messages
            if has_message {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue; // Skip to next iteration to process more messages
            }
            
            // Add a small delay to avoid spinning the CPU at 100% when checking for messages
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Read with timeout to avoid hanging forever
        let read_result = timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            reader.read(&mut buf)
        ).await;
        
        let n = match read_result {
            Ok(Ok(0)) => {
                // Connection closed by client
                debug!("Connection closed by client: {}", addr);
                
                // Clean up subscriptions
                kv.connection_manager.remove_client(&client_id);
                break;
            },
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                error!("Error reading from socket {}: {}", addr, e);
                return Err(e.into());
            },
            Err(_) => {
                // Timeout occurred
                warn!("Connection timeout for client: {}", addr);
                break;
            }
        };
        
        // Process the command
        let input = String::from_utf8_lossy(&buf[..n]);
        
        if debug_mode {
            debug!("Received {} bytes from {}", n, addr);
            debug!("Raw input: {:?}", input);
        }
        
        let start = Instant::now();
        
        // Use the command as-is, without appending client ID
        let command_to_execute = input.to_string();
        
        // Create or get connection context for this client
        if !kv.connection_manager.has_client(&client_id) {
            debug!("Creating new connection context for client {}", client_id);
            kv.connection_manager.create_client(&client_id);
        } else {
            debug!("Found existing connection context for client {}", client_id);
        }
        
        // Get connection context
        let mut response = if let Some(context) = kv.connection_manager.get_client(&client_id) {
            // Execute command with connection context
            debug!("Executing command with context for client {}: {}", client_id, command_to_execute);
            kv.handle_command_with_context(&command_to_execute, Some(&context))
        } else {
            // Fallback to handle command without context
            debug!("Failed to get context for client {}, executing without context: {}", client_id, command_to_execute);
            kv.handle_command(&command_to_execute)
        };
        let duration = start.elapsed();
        
        // Always refresh receivers after any command to catch subscription changes
        // Clear existing receivers (we'll recreate them)
        receivers.clear();
        
        // Get the connection context to get subscribed channels
        if let Some(context) = kv.connection_manager.get_client(&client_id) {
            // Get the list of channels this client is subscribed to
            let channels = context.get_subscriptions();
            
            // Create a receiver for each channel
            for channel in channels {
                let tx = chabi_core::commands::handlers::get_or_create_channel(&kv.channels, &channel);
                let rx = tx.subscribe();
                receivers.push(rx);
                
                // Always log subscription details to debug PubSub issues
                debug!("Created/refreshed receiver for channel '{}' for client {} (current receivers: {})", 
                      channel, client_id, receivers.len());
            }
        }
        
        if debug_mode {
            debug!("Command processed in {:?}", duration);
        }
        
        // Ensure response ends with CRLF
        if !response.ends_with("\r\n") {
            response.push_str("\r\n");
        }
        
        // Write response and flush
        if let Err(e) = writer.write_all(response.as_bytes()).await {
            error!("Failed to write response to {}: {}", addr, e);
            return Err(e.into());
        }
        
        if let Err(e) = writer.flush().await {
            error!("Failed to flush response to {}: {}", addr, e);
            return Err(e.into());
        }
        
        if debug_mode {
            debug!("Response sent to {} ({} bytes)", addr, response.len());
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let matches = App::new("Chabi KV Server")
        .version("0.1.0")
        .author("EnvSync-Cloud")
        .about("Redis-compatible KV store")
        .arg(Arg::new("debug")
            .long("debug")
            .short('d')
            .help("Enable debug output")
            .takes_value(false))
        .arg(Arg::new("host")
            .long("host")
            .help("Host to bind to")
            .takes_value(true))
        .arg(Arg::new("port")
            .long("port")
            .short('p')
            .help("Port to listen on for Redis protocol")
            .takes_value(true))
        .arg(Arg::new("http-port")
            .long("http-port")
            .help("Port to listen on for HTTP API")
            .takes_value(true))
        .arg(Arg::new("data-dir")
            .long("data-dir")
            .help("Directory for data storage")
            .takes_value(true))
        .arg(Arg::new("persist-interval")
            .long("persist-interval")
            .help("Auto-persist interval in milliseconds")
            .takes_value(true))
        .get_matches();
    
    // Get configuration from command line arguments and environment variables
    let debug_mode = matches.is_present("debug") || 
        std::env::var("DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false);
    
    // Initialize logging based on debug mode
    if debug_mode {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    }
    
    let data_dir = matches.value_of("data-dir")
        .map(String::from)
        .or_else(|| std::env::var("DATA_DIR").ok())
        .unwrap_or_else(|| "data".to_string());
    
    let host = matches.value_of("host")
        .map(String::from)
        .or_else(|| std::env::var("HOST").ok())
        .unwrap_or_else(|| DEFAULT_HOST.to_string());
    
    let port = matches.value_of("port")
        .map(String::from)
        .or_else(|| std::env::var("PORT").ok())
        .or_else(|| std::env::var("CHABI_PORT").ok())
        .unwrap_or_else(|| "7379".to_string());
        
    // Temporarily not used since HTTP server is disabled
    let _http_port = matches.value_of("http-port")
        .map(String::from)
        .or_else(|| std::env::var("HTTP_PORT").ok())
        .or_else(|| std::env::var("CHABI_HTTP_PORT").ok())
        .unwrap_or_else(|| "7380".to_string());
    
    let persist_interval = matches.value_of("persist-interval")
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| std::env::var("PERSIST_INTERVAL_MS").ok().and_then(|v| v.parse::<u64>().ok()))
        .unwrap_or(5000);
    
    // Setup data directory
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("Failed to create data directory: {}", data_dir))?;
    
    // Configure persistence
    let persistence_options = PersistenceOptions {
        path: format!("{}/chabi_data.bin", data_dir),
        auto_persist: true,
        persist_interval_ms: Some(persist_interval),
    };
    
    // Create KV store with persistence and debug mode, wrap in Arc for thread safety
    let kv = Arc::new(ChabiKV::new_with_debug(Some(persistence_options), debug_mode));
    
    // Start TCP listener
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {}", addr))?;
    
    info!("Chabi KV Store listening on {} (Redis CLI compatible)", addr);
    // Temporarily disabled HTTP server
    // info!("HTTP API available on http://{}:{}", host, http_port);
    info!("Data will be saved to: {}/chabi_data.bin", data_dir);
    info!("Auto-persist interval: {}ms", persist_interval);
    if debug_mode {
        info!("Debug mode: enabled");
    }
    
    // Temporarily disabled HTTP server
    /*
    // Start HTTP server in a separate task
    let kv_clone = kv.clone();
    let http_host = host.clone();
    let http_port_parsed = http_port.parse::<u16>()
        .with_context(|| format!("Invalid HTTP port number: {}", http_port))?;
    tokio::spawn(async move {
        if let Err(e) = http::run_http_server(kv_clone, http_host, http_port_parsed, debug_mode).await {
            error!("HTTP server error: {}", e);
        }
    });
    */
    
    // Print supported commands by category
    info!("Supported commands:");
    info!("  Connection: PING, ECHO");
    info!("  Strings: GET, SET, SETEX, INCR, DECR, APPEND, STRLEN");
    info!("  Hashes: HGET, HSET, HSETEX, HGETALL, HEXISTS, HDEL, HLEN, HKEYS, HVALS");
    info!("  Lists: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN");
    info!("  Sets: SADD, SMEMBERS, SISMEMBER, SCARD, SREM");
    info!("  Keys: DEL, EXISTS, KEYS, TTL, EXPIRE, RENAME, TYPE");
    info!("  PubSub: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PUBSUB");
    info!("  Server: INFO, SAVE");
    info!("  Documentation: DOCS, COMMAND");
    
    // Connection limiter to prevent resource exhaustion
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    
    info!("Ready to accept connections (limit: {} concurrent connections)", MAX_CONCURRENT_CONNECTIONS);
    
    loop {
        // Wait for a permit to be available before accepting a new connection
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(e) => {
                error!("Semaphore acquire error: {}", e);
                // Short delay to avoid tight loop in case of semaphore errors
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        
        // Accept a new connection
        let accept_result = listener.accept().await;
        match accept_result {
            Ok((socket, addr)) => {
                info!("New connection from: {}", addr);
                
                // Clone necessary resources for the new task
                let kv_clone = kv.clone();
                
                // Spawn a new task to handle the client
                tokio::spawn(async move {
                    // The permit is automatically released when dropped (when this task completes)
                    let _permit = permit;
                    
                    if let Err(e) = handle_client(socket, addr, kv_clone, debug_mode).await {
                        error!("Error handling client {}: {}", addr, e);
                    }
                    
                    debug!("Connection handling completed for {}", addr);
                });
            },
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}
