#!/bin/bash
set -e

BROKER="kafka:29092"

echo "📋 Creating application Kafka topics on $BROKER ..."

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic products --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic enriched-orders --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic fraud-alerts --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic category-sales --partitions 3 --replication-factor 1

echo "✅ Topics created:"
kafka-topics --bootstrap-server $BROKER --list

