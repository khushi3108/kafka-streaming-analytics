#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
#  Full Demo Script – E-Commerce Kafka Streaming
#  Usage: bash scripts/demo.sh
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

API="http://localhost:8080"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

log()  { echo -e "${GREEN}[DEMO]${NC} $*"; }
step() { echo -e "\n${YELLOW}══════════════════════════════════════════${NC}"; echo -e "${CYAN}  STEP: $*${NC}"; echo -e "${YELLOW}══════════════════════════════════════════${NC}"; }

# ─── Step 0: Health check ────────────────────────────────────────────
step "0 – Verify API is up"
curl -sf "$API/actuator/health" | python3 -m json.tool || \
  { echo "API not running. Start with: ./gradlew bootRun"; exit 1; }
log "API is healthy ✅"

# ─── Step 1: Post a single manual order ──────────────────────────────
step "1 – Publish a single order (MacBook Pro)"
curl -s -X POST "$API/api/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "C001",
    "productId":  "P001",
    "category":   "Electronics",
    "amount":     2499.99,
    "quantity":   1
  }' | python3 -m json.tool
log "Order published ✅"

# ─── Step 2: Publish a batch of orders ───────────────────────────────
step "2 – Publish a batch of 5 orders"
curl -s -X POST "$API/api/orders/batch" \
  -H "Content-Type: application/json" \
  -d '[
    {"customerId":"C002","productId":"P003","category":"Electronics","amount":349.99,"quantity":1},
    {"customerId":"C003","productId":"P009","category":"Clothing","amount":69.99,"quantity":2},
    {"customerId":"C004","productId":"P012","category":"Books","amount":35.99,"quantity":3},
    {"customerId":"C005","productId":"P015","category":"Food","amount":24.99,"quantity":1},
    {"customerId":"C006","productId":"P018","category":"Sports","amount":59.99,"quantity":1}
  ]' | python3 -m json.tool

# ─── Step 3: Trigger a high-value order (fraud) ───────────────────────
step "3 – Trigger a HIGH-VALUE order to fire fraud alert"
curl -s -X POST "$API/api/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "C007",
    "productId":  "P001",
    "category":   "Electronics",
    "amount":     7499.97,
    "quantity":   3
  }' | python3 -m json.tool
log "High-value order sent → check fraud-alerts topic ✅"

# ─── Step 4: Simulate burst of random orders ─────────────────────────
step "4 – Simulate 30 random orders (burst)"
curl -s -X POST "$API/api/orders/simulate?count=30" | python3 -m json.tool
log "Burst simulation done ✅"

# ─── Step 5: Start continuous simulation ─────────────────────────────
step "5 – Start continuous simulation (1 order / 1500ms)"
curl -s -X POST "$API/api/orders/simulate/start?intervalMs=1500" | python3 -m json.tool
log "Simulation running… sleeping 15s to accumulate events"
sleep 15

# ─── Step 6: Query analytics ─────────────────────────────────────────
step "6 – Query category-sales state store"
curl -s "$API/api/analytics/category-sales" | python3 -m json.tool

step "7 – Query customer spending"
curl -s "$API/api/analytics/customer-spending" | python3 -m json.tool

step "8 – KafkaStreams state"
curl -s "$API/api/analytics/streams/status" | python3 -m json.tool

# ─── Step 9: Stop simulation ─────────────────────────────────────────
step "9 – Stop continuous simulation"
curl -s -X POST "$API/api/orders/simulate/stop" | python3 -m json.tool

echo ""
log "🎉 Demo complete! Open Kafka UI at http://localhost:9090 to browse topics."
log "   Run ksqlDB queries: docker exec -it ksqldb-cli ksql http://ksqldb-server:8088"

