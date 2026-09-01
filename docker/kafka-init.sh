#!/bin/bash
set -e

# All three brokers, so topic creation still works if any single broker is down.
BROKER="kafka-1:29092,kafka-2:29093,kafka-3:29094"

# Durability settings, matching KafkaTopicConfig.java and the broker defaults in
# docker-compose.yml. RF=1 is not compatible with the EXACTLY_ONCE_V2 guarantee the Streams
# app now makes: a transaction is only as durable as the partitions it writes to.
RF=3
MIN_ISR="min.insync.replicas=2"

echo "📋 Creating application Kafka topics on $BROKER (RF=$RF, $MIN_ISR) ..."

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor $RF \
  --config $MIN_ISR

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic products --partitions 3 --replication-factor $RF \
  --config cleanup.policy=compact \
  --config $MIN_ISR

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic enriched-orders --partitions 3 --replication-factor $RF \
  --config $MIN_ISR

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic fraud-alerts --partitions 1 --replication-factor $RF \
  --config $MIN_ISR

kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic category-sales --partitions 3 --replication-factor $RF \
  --config $MIN_ISR

# Dead-letter topic: raw bytes + failure metadata for records that failed deserialization.
# Written by DeadLetterDeserializationExceptionHandler. Retained for 30 days (longer than the
# brokers' 7-day default) because its purpose is post-mortem analysis and replay after a fix.
kafka-topics --bootstrap-server $BROKER --create --if-not-exists \
  --topic dead-letter --partitions 3 --replication-factor $RF \
  --config $MIN_ISR \
  --config retention.ms=2592000000

echo "✅ Topics created:"
kafka-topics --bootstrap-server $BROKER --list
