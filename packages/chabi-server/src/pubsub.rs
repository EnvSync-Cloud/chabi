use chabi_core::protocol::array_response;
use tokio::sync::mpsc;

pub async fn handle_pubsub(
    mut writer: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
    client_id: &str,
    kv: std::sync::Arc<chabi_core::ChabiKV>,
    command: &str,
    channel_names: Vec<String>,
    debug_mode: bool,
) {
    use tokio::io::AsyncWriteExt;
    
    // Create a channel for forwarding published messages to this task
    let (tx, mut rx) = mpsc::channel::<(String, String)>(100);
    
    // Create receivers for all subscribed channels
    let mut handles = Vec::new();
    
    for channel_name in channel_names {
        // Get the channel sender
        let sender = {
            let channels = kv.channels.lock().unwrap();
            if let Some(tx) = channels.get(&channel_name) {
                tx.clone()
            } else {
                // Channel doesn't exist yet, create it
                drop(channels); // Release the lock
                chabi_core::commands::handlers::get_or_create_channel(&kv.channels, &channel_name)
            }
        };
        
        // Create a receiver
        let mut rx = sender.subscribe();
        let tx_clone = tx.clone();
        let channel_name_clone = channel_name.clone();
        
        // Start a task to listen for messages on this channel
        let handle = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        // Forward the message to the main task
                        if let Err(_) = tx_clone.send((channel_name_clone.clone(), message)).await {
                            // If we can't send, the main task has probably shut down
                            break;
                        }
                    },
                    Err(_) => {
                        // Channel closed or error occurred
                        break;
                    }
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Process messages
    loop {
        tokio::select! {
            Some((channel, message)) = rx.recv() => {
                if debug_mode {
                    log::debug!("Forwarding message to client {}: {} on {}", client_id, message, channel);
                }
                
                // Format message as Redis PUB/SUB message
                let mut message_response = Vec::new();
                message_response.push("message".to_string());
                message_response.push(channel);
                message_response.push(message);
                
                let formatted_response = array_response(&message_response);
                
                // Send the message to the client
                if let Err(e) = writer.write_all(formatted_response.as_bytes()).await {
                    if debug_mode {
                        log::error!("Failed to write to client {}: {}", client_id, e);
                    }
                    break;
                }
                
                if let Err(e) = writer.flush().await {
                    if debug_mode {
                        log::error!("Failed to flush to client {}: {}", client_id, e);
                    }
                    break;
                }
            },
            else => {
                // All channels have been closed
                break;
            }
        }
    }
    
    // Clean up
    for handle in handles {
        handle.abort();
    }
    
    // Clean up subscriptions
    kv.connection_manager.remove_client(client_id);
}
