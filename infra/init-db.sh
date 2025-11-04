#!/bin/bash
# Phase 1: Database initialization script
# This script runs the SQL schema if tables don't exist yet

echo "Checking if database schema needs initialization..."

# Check if messages table exists
TABLE_EXISTS=$(docker exec postgres psql -U pipeline_user -d email_pipeline -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'messages');")

if [ "$TABLE_EXISTS" = "t" ]; then
    echo "✓ Database schema already initialized"
else
    echo "Initializing database schema..."
    cat infra/init.sql | docker exec -i postgres psql -U pipeline_user -d email_pipeline
    echo "✓ Database schema initialized successfully"
fi


