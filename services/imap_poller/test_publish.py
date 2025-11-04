"""
Test Publisher - Phase 2
Publishes a test message to raw_emails.v1 stream
"""

import redis
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Redis Connections
# When running locally, use 'localhost'. When running in Docker, use 'redis'
r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),  # Default 'localhost' for local testing
    port=int(os.getenv('REDIS_PORT', 6379)),
    decode_responses=True  # Get strings, not bytes
)

# Publishing #XADD
message_id = r.xadd('raw_emails.v1', {
    'trace_id': 'test-123',
    'mailbox_id': 'test@gmail.com',
    'subject': 'Test Email'
})
print(f"Published message: {message_id}")

# Consuming (XREADGROUP)
messages = r.xreadgroup(
    'normalizer-g', # Group name
    'normalizer-1', # Consumer name
    {'raw_emails.v1': '>'}, # Stream: Fixed typo - 'raw_emails.v1' (with 's')
    count=1,
    block=1000 # wait 1 second for messages
)

if messages:
    stream, data = messages[0]
    message_id, fields = data[0]
    
    # Process message
    print(f"Received message {message_id}: {fields}")
    
    # ACK it - This tells Redis "I'm done processing this message"
    # It removes the message_id(redis_id) from pending list
    # Without ACK, message stays in PENDING and can be retried
    r.xack('raw_emails.v1', 'normalizer-g', message_id)  # Fixed typo
    print(f"✓ Acknowledged message {message_id}")