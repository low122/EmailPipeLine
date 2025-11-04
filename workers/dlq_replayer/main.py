"""
dlq_worker Worker - Phase 0 Stub

This worker will replay failed messages from the dead letter queue

Phase 0: Just a stub for orchestration testing

Author: Low, Jiat Zin
Phase: 0 - Mono-repo Scaffold
"""

import structlog
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure structured logging for consistent output across all services
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),      # Add ISO timestamp
        structlog.processors.add_log_level,                # Add log level (INFO, ERROR, etc.)
        structlog.dev.ConsoleRenderer()                    # Human-readable format for dev
    ]
)

# Create logger bound to this service's name
log = structlog.get_logger(service="dlq_replayer")


def main():
    """
    Main service loop.

    Phase 0: Just logs startup and runs indefinitely
    Future phases: Will parse MIME, compute body_hash, emit to emails.normalized.v1
    """
    # Log service startup
    log.info("dlq_replayer starting...")

    # Simulate initialization (future: connect to IMAP, validate config)
    time.sleep(2)

    # Log ready state (used by Docker healthcheck)
    log.info("dlq_replayer ready")

    # Keep service running with periodic heartbeat
    # Future: This will be replaced by actual IMAP polling loop
    while True:
        time.sleep(30)
        log.info("dlq_replayer alive")


if __name__ == "__main__":
    main()