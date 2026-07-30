#!/bin/bash
# Startup script for Kafka Connect with Debezium installation
# This script runs inside the Docker container on startup

echo "========================================"
echo "Starting Kafka Connect with Debezium"
echo "========================================"

# Debezium version
DEBEZIUM_VERSION="2.4.2.Final"

# Check if connectors already installed
if [ -d "/usr/share/confluent-hub-components/debezium-postgres" ] && [ -d "/usr/share/confluent-hub-components/debezium-mysql" ]; then
    echo "Debezium connectors already installed, skipping download..."
else
    echo "Installing Debezium connectors..."
    
    # Create plugin directory
    mkdir -p /usr/share/confluent-hub-components
    
    # Download PostgreSQL connector
    echo "Downloading PostgreSQL connector..."
    curl -sL "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" -o /tmp/postgres-connector.tar.gz
    
    # Download MySQL connector  
    echo "Downloading MySQL connector..."
    curl -sL "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/${DEBEZIUM_VERSION}/debezium-connector-mysql-${DEBEZIUM_VERSION}-plugin.tar.gz" -o /tmp/mysql-connector.tar.gz
    
    # Extract PostgreSQL connector
    echo "Extracting PostgreSQL connector..."
    mkdir -p /usr/share/confluent-hub-components/debezium-postgres
    tar -xzf /tmp/postgres-connector.tar.gz -C /usr/share/confluent-hub-components/debezium-postgres --strip-components=1
    
    # Extract MySQL connector
    echo "Extracting MySQL connector..."
    mkdir -p /usr/share/confluent-hub-components/debezium-mysql
    tar -xzf /tmp/mysql-connector.tar.gz -C /usr/share/confluent-hub-components/debezium-mysql --strip-components=1
    
    # Cleanup
    rm /tmp/postgres-connector.tar.gz /tmp/mysql-connector.tar.gz
    
    echo "Debezium connectors installed successfully!"
fi

# List installed connectors
echo "Installed connectors:"
ls -la /usr/share/confluent-hub-components/

echo "Starting Kafka Connect..."
# Start Kafka Connect
exec /etc/confluent/docker/run
