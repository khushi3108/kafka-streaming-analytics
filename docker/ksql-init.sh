#!/bin/bash
# ─────────────────────────────────────────────────────────────────────
#  ksqlDB Init Script
#  Pipes init.sql into the ksqlDB CLI.  The CLI runs non-interactively
#  when stdin is not a TTY, processing each statement in sequence.
# ─────────────────────────────────────────────────────────────────────

KSQL_SERVER="http://ksqldb-server:8088"

echo "⏳ Waiting for ksqlDB server to be ready..."
until curl -sf "$KSQL_SERVER/info" > /dev/null 2>&1; do
  echo "  still waiting..."
  sleep 3
done

echo "✅ ksqlDB is up. Running init.sql..."
ksql "$KSQL_SERVER" < /ksql/init.sql

echo "✅ ksqlDB streams and tables created successfully."

