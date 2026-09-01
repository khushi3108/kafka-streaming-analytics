-- ═══════════════════════════════════════════════════════════════════════
--  ksqlDB Demo Queries  – run interactively in the ksqlDB CLI
--  Access CLI:  docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
-- ═══════════════════════════════════════════════════════════════════════

SET 'auto.offset.reset' = 'earliest';

-- ── Explore available streams / tables ─────────────────────────────────
SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;
DESCRIBE orders_stream;

-- ═══════════════════════════════════════════════════════════════════════
--  SECTION A: Basic Stream Queries
-- ═══════════════════════════════════════════════════════════════════════

-- A1. Watch all orders as they arrive (push query – live feed)
SELECT order_id, customer_id, category, amount, timestamp
FROM orders_stream
EMIT CHANGES
LIMIT 10;

-- A2. Filter: only Electronics orders
SELECT order_id, customer_id, amount
FROM orders_stream
WHERE category = 'Electronics'
EMIT CHANGES
LIMIT 5;

-- A3. Orders above $500 (fraud candidates)
SELECT order_id, customer_id, category, amount
FROM orders_stream
WHERE amount > 500
EMIT CHANGES;

-- A4. Pull query on enriched orders (snapshot, not continuous)
SELECT order_id, product_name, brand, category, amount
FROM enriched_orders_stream
WHERE category = 'Electronics'
LIMIT 5;

-- ═══════════════════════════════════════════════════════════════════════
--  SECTION B: Windowed Aggregations
-- ═══════════════════════════════════════════════════════════════════════

-- B1. Real-time category totals – 1-minute tumbling window (push)
SELECT category, order_count, total_sales, avg_order_value
FROM category_sales_1min
EMIT CHANGES;

-- B2. Pull query: snapshot of all category aggregations right now
SELECT category, order_count, total_sales, avg_order_value, total_items
FROM category_sales_1min;

-- B3. Pull query: just Electronics
SELECT category, order_count, total_sales
FROM category_sales_1min
WHERE category = 'Electronics';

-- B4. Hourly category sales (hopping window)
SELECT category, order_count, total_sales
FROM category_sales_hourly
EMIT CHANGES
LIMIT 20;

-- ═══════════════════════════════════════════════════════════════════════
--  SECTION C: Customer Analytics
-- ═══════════════════════════════════════════════════════════════════════

-- C1. Running customer totals (push query)
SELECT customer_id, total_orders, total_spent, largest_order
FROM customer_spending_total
EMIT CHANGES
LIMIT 15;

-- C2. Pull query: spending for a specific customer
SELECT customer_id, total_orders, total_spent, avg_order_value
FROM customer_spending_total
WHERE customer_id = 'C001';

-- C3. Count total orders and revenue across all customers
SELECT COUNT(*) AS total_orders, SUM(amount) AS total_revenue
FROM orders_stream
EMIT CHANGES;

-- ═══════════════════════════════════════════════════════════════════════
--  SECTION D: Fraud Analytics
-- ═══════════════════════════════════════════════════════════════════════

-- D1. Watch fraud alerts live
SELECT alert_id, order_id, customer_id, category, amount, severity
FROM fraud_alerts_stream
EMIT CHANGES;

-- D2. Fraud by category (table)
SELECT category, alert_count, flagged_amount, max_flagged
FROM fraud_by_category
EMIT CHANGES;

-- D3. Pull: which categories have the most fraud?
SELECT category, alert_count, flagged_amount
FROM fraud_by_category
WHERE alert_count > 1;

-- ═══════════════════════════════════════════════════════════════════════
--  SECTION E: Ad-hoc / One-time Queries
-- ═══════════════════════════════════════════════════════════════════════

-- E1. Orders grouped by status
SELECT status, COUNT(*) AS count
FROM orders_stream
GROUP BY status
EMIT CHANGES;

-- E2. Compute revenue per product (from enriched stream)
SELECT product_name, COUNT(*) AS sold, SUM(amount) AS revenue
FROM enriched_orders_stream
GROUP BY product_name
EMIT CHANGES
LIMIT 20;

-- E3. Top spending customer (one-time window query)
SELECT customer_id, total_spent
FROM customer_spending_total
WHERE total_spent > 1000;

