#!/usr/bin/env python3
import redis
import threading
import time
import sys

def subscriber_thread():
    r = redis.Redis(host='localhost', port=7375)
    pubsub = r.pubsub()
    pubsub.subscribe('test-channel')
    
    print("Subscriber is listening...")
    
    # Process messages
    for message in pubsub.listen():
        print(f"Subscriber received: {message}")
        if message.get('data') == b'QUIT':
            break

def publisher_thread():
    r = redis.Redis(host='localhost', port=7375)
    
    # Wait a bit for subscriber to connect
    time.sleep(1)
    
    # Publish a few messages
    for i in range(5):
        message = f"Test message {i}"
        print(f"Publishing: {message}")
        r.publish('test-channel', message)
        time.sleep(1)
    
    # Send quit message
    r.publish('test-channel', 'QUIT')

if __name__ == '__main__':
    # Start subscriber thread
    sub_thread = threading.Thread(target=subscriber_thread)
    sub_thread.daemon = True
    sub_thread.start()
    
    # Start publisher thread
    pub_thread = threading.Thread(target=publisher_thread)
    pub_thread.daemon = True
    pub_thread.start()
    
    # Wait for both threads to complete
    try:
        while sub_thread.is_alive() or pub_thread.is_alive():
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(0)
