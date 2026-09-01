#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
#  Setup Script – Downloads Gradle wrapper jar and verifies prerequisites
# ════════════════════════════════════════════��══════════════════════════
set -euo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✅  $*${NC}"; }
warn() { echo -e "${YELLOW}⚠️   $*${NC}"; }
err()  { echo -e "${RED}❌  $*${NC}"; exit 1; }

echo "================================================================"
echo "  E-Commerce Kafka Streaming – Project Setup"
echo "================================================================"

# ── Check Java 17+ ───────────────────────────────────────────────────
if ! command -v java &> /dev/null; then err "Java not found. Install Java 17+."; fi
JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
[ "$JAVA_VER" -ge 17 ] && ok "Java $JAVA_VER found" || err "Java 17+ required (found $JAVA_VER)"

# ── Check Docker ─────────────────────────────────────────────────────
if ! command -v docker &> /dev/null; then err "Docker not found. Install Docker Desktop."; fi
ok "Docker found"

if ! docker info &> /dev/null; then err "Docker daemon not running. Start Docker Desktop."; fi
ok "Docker daemon running"

# ── Download Gradle wrapper jar if missing ───────────────────────────
WRAPPER_JAR="gradle/wrapper/gradle-wrapper.jar"
if [ ! -f "$WRAPPER_JAR" ]; then
  warn "gradle-wrapper.jar not found. Downloading..."
  mkdir -p gradle/wrapper
  curl -fsSL \
    "https://raw.githubusercontent.com/gradle/gradle/v8.5.0/gradle/wrapper/gradle-wrapper.jar" \
    -o "$WRAPPER_JAR" \
    || warn "Could not auto-download wrapper. Run: gradle wrapper --gradle-version 8.5"
  [ -f "$WRAPPER_JAR" ] && ok "gradle-wrapper.jar downloaded" || warn "Using system Gradle"
else
  ok "gradle-wrapper.jar present"
fi

# ── Make scripts executable ──────────────────────────────────────────
chmod +x gradlew scripts/demo.sh 2>/dev/null || true
ok "Scripts made executable"

# ── Pull Docker images ───────────────────────────────────────────────
echo ""
echo "Pulling Docker images (this may take a few minutes on first run)..."
docker compose pull
ok "Docker images ready"

echo ""
echo "================================================================"
echo "  Setup complete! To run the project:"
echo ""
echo "  1. Start infrastructure:"
echo "     docker compose up -d"
echo ""
echo "  2. Build & run the Spring Boot app:"
echo "     ./gradlew bootRun"
echo ""
echo "  3. In another terminal, run the demo:"
echo "     bash scripts/demo.sh"
echo ""
echo "  4. Access ksqlDB CLI:"
echo "     docker exec -it ksqldb-cli ksql http://ksqldb-server:8088"
echo "================================================================"

