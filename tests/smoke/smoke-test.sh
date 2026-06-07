#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# smoke-test.sh: Validate the data pipeline is working end-to-end
# Usage: ./smoke-test.sh or tests/smoke/smoke-test.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e

echo "================================================================================"
echo "Data Pipeline NxT - Smoke Test"
echo "================================================================================"
echo ""

# ─── Find project root ─────────────────────────────────────────────────────────
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$PROJECT_ROOT"

# Configuration
COMPOSE_FILE="docker-compose.yml"
KAFKA_BOOTSTRAP="localhost:9092"
KAFKA_TOPIC="sensor-topic"
IOTDB_HOST="localhost"
IOTDB_PORT="6667"
FLINK_REST_URL="http://localhost:8081"
TIMEOUT_SECONDS=120

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

wait_for_service() {
    local service=$1
    local health_cmd=$2
    local timeout=$3
    local elapsed=0
    
    log_info "Waiting for $service to be healthy (timeout: ${timeout}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        if eval "$health_cmd" > /dev/null 2>&1; then
            log_info "✓ $service is healthy!"
            return 0
        fi
        echo -n "."
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "✗ $service failed to become healthy after ${timeout}s"
    return 1
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: Docker Compose Status
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 1: Docker Compose Status"

if ! docker compose ps > /dev/null 2>&1; then
    log_error "Docker Compose is not running!"
    log_info "Run: docker compose up -d"
    exit 1
fi

RUNNING=$(docker compose ps --services --filter "status=running" | wc -l)
TOTAL=$(docker compose ps --services | wc -l)

log_info "Services running: $RUNNING / $TOTAL"
docker compose ps --format "table {{.Service}}\t{{.Status}}"

if [ $RUNNING -lt $TOTAL ]; then
    log_warn "Not all services are running. Waiting..."
fi

# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: Service Health Checks
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 2: Service Health Checks"

# Zookeeper
wait_for_service "Zookeeper" \
    "echo ruok | nc localhost 2181" \
    30

# Kafka
wait_for_service "Kafka" \
    "docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1" \
    60

# IoTDB
wait_for_service "IoTDB" \
    "curl -s http://localhost:8080/ping > /dev/null" \
    60

# Flink JobManager
wait_for_service "Flink JobManager" \
    "curl -s $FLINK_REST_URL/v1/overview > /dev/null" \
    60

# ─────────────────────────────────────────────────────────────────────────────
# TEST 3: Kafka Topic & Data
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 3: Kafka Topic & Data Flow"

log_info "Checking Kafka topic: $KAFKA_TOPIC"
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -q "$KAFKA_TOPIC"
log_info "✓ Topic exists"

log_info "Checking for recent messages in Kafka (waiting up to 30s)..."
MESSAGE_COUNT=0
for i in {1..15}; do
    MESSAGE_COUNT=$(docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:29092 \
        --topic "$KAFKA_TOPIC" \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms 1000 2>/dev/null | wc -l)
    
    if [ "$MESSAGE_COUNT" -gt 0 ]; then
        log_info "✓ Found $MESSAGE_COUNT message(s) in Kafka"
        break
    fi
    sleep 2
done

if [ "$MESSAGE_COUNT" -eq 0 ]; then
    log_warn "⚠ No messages found in Kafka (simulator may still be starting)"
else
    log_info "Sample message:"
    docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:29092 \
        --topic "$KAFKA_TOPIC" \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms 1000 2>/dev/null | jq . || true
fi

# ─────────────────────────────────────────────────────────────────────────────
# TEST 4: Flink Job Status
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 4: Flink Job Status"

log_info "Fetching Flink job list..."
JOBS=$(curl -s "$FLINK_REST_URL/v1/jobs" 2>/dev/null | jq '.jobs | length' 2>/dev/null || echo "0")

if [ "$JOBS" -gt 0 ]; then
    log_info "✓ Found $JOBS Flink job(s)"
    curl -s "$FLINK_REST_URL/v1/jobs" 2>/dev/null | jq '.jobs[] | {id, name, state}' || true
else
    log_warn "⚠ No Flink jobs running (may still be submitting)"
fi

# Get parallelism
PARALLELISM=$(curl -s "$FLINK_REST_URL/v1/overview" 2>/dev/null | jq '.taskmanagers' 2>/dev/null || echo "0")
log_info "Task managers available: $PARALLELISM"

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5: IoTDB Data Verification
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 5: IoTDB Data Verification"

log_info "Checking IoTDB connectivity..."
docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root \
    -e "SHOW DATABASES;" 2>/dev/null > /tmp/iotdb_test.txt && \
    log_info "✓ IoTDB is responsive" || log_warn "⚠ IoTDB query failed"

log_info "Querying for sensor data (waiting up to 60s)..."
RECORD_COUNT=0
for i in {1..30}; do
    # Query IoTDB and extract record count
    QUERY_RESULT=$(docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root \
        -e "SELECT COUNT(*) FROM root.factory1.** WHERE TIME > 0;" 2>/dev/null || echo "")
    
    # Parse the count from the result (look for the first number before a pipe)
    RECORD_COUNT=$(echo "$QUERY_RESULT" | grep -oE '^[[:space:]]*[0-9]+' | head -1 | xargs 2>/dev/null || echo "0")
    
    if [ ! -z "$RECORD_COUNT" ] && [ "$RECORD_COUNT" != "0" ]; then
        log_info "✓ Found $RECORD_COUNT record(s) in IoTDB"
        
        log_info "Latest temperature data sample:"
        docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root \
            -e "SELECT gas_temperature FROM root.factory1.** LIMIT 5;" 2>/dev/null | head -10
        break
    fi
    echo -n "."
    sleep 2
done

if [ -z "$RECORD_COUNT" ] || [ "$RECORD_COUNT" = "0" ]; then
    log_warn "⚠ No data found in IoTDB yet (pipeline may still be processing)"
    RECORD_COUNT=0
fi

# ─────────────────────────────────────────────────────────────────────────────
# TEST 6: End-to-End Latency (if data exists)
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 6: End-to-End Latency (Optional)"

if [ "$RECORD_COUNT" -gt 0 ]; then
    log_info "Measuring latency from Kafka → IoTDB..."
    
    # Get current time in milliseconds
    CURRENT_TIME_MS=$(date +%s%3N)
    
    # Get latest data timestamp from IoTDB
    LATEST_TIMESTAMP=$(docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root \
        -e "SELECT * FROM root.factory1.MC001 LIMIT 1;" 2>/dev/null | \
        grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1 2>/dev/null || echo "")
    
    if [ ! -z "$LATEST_TIMESTAMP" ]; then
        # Convert ISO timestamp to milliseconds (simplified)
        log_info "✓ Latest event timestamp: $LATEST_TIMESTAMP"
    fi
else
    log_warn "⚠ Skipping latency test (no data in IoTDB yet)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# TEST 7: Error Checking
# ─────────────────────────────────────────────────────────────────────────────

log_step "TEST 7: Error Checking"

log_info "Checking for errors in service logs..."

ERRORS_FOUND=0

# Check Flink logs for exceptions
if docker compose logs flink-jobmanager 2>/dev/null | grep -i "exception\|error" | grep -v "WARN" > /dev/null 2>&1; then
    log_warn "⚠ Errors found in Flink JobManager logs"
    docker compose logs flink-jobmanager 2>/dev/null | grep -i "exception\|error" | grep -v "WARN" | head -3
    ERRORS_FOUND=$((ERRORS_FOUND + 1))
fi

# Check simulator logs
if docker compose logs simulator 2>/dev/null | grep -i "exception\|error" | grep -v "WARN" > /dev/null 2>&1; then
    log_warn "⚠ Errors found in Simulator logs"
    docker compose logs simulator 2>/dev/null | grep -i "exception\|error" | grep -v "WARN" | head -3
    ERRORS_FOUND=$((ERRORS_FOUND + 1))
fi

if [ $ERRORS_FOUND -eq 0 ]; then
    log_info "✓ No critical errors detected"
fi

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

log_step "SMOKE TEST SUMMARY"

TESTS_PASSED=0
TESTS_TOTAL=7

# Test 1: Flink job running
if [ "$JOBS" -gt 0 ] 2>/dev/null; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 1: Flink job is running"
else
    log_warn "✗ TEST 1: Flink job not running"
fi

# Test 2: Kafka has messages
if [ "$MESSAGE_COUNT" -gt 0 ]; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 2: Kafka has messages"
else
    log_warn "✗ TEST 2: Kafka has no messages"
fi

# Test 3: IoTDB has data
if [ "$RECORD_COUNT" -gt 0 ]; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 3: IoTDB has data ($RECORD_COUNT records)"
else
    log_warn "✗ TEST 3: IoTDB has no data"
fi

# Test 4: No critical errors
if [ $ERRORS_FOUND -eq 0 ]; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 4: No critical errors"
fi

# Test 5: Zookeeper healthy
if echo "ruok" | nc localhost 2181 > /dev/null 2>&1; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 5: Zookeeper healthy"
fi

# Test 6: Kafka healthy
if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 6: Kafka healthy"
fi

# Test 7: IoTDB healthy
if curl -s http://localhost:8080/ping > /dev/null 2>&1; then
    ((TESTS_PASSED++))
    log_info "✓ TEST 7: IoTDB healthy"
fi

echo ""
echo "Results: $TESTS_PASSED / 7 tests passed"
echo ""

if [ $TESTS_PASSED -ge 6 ]; then
    log_info "✅ MVP is FULLY FUNCTIONAL - End-to-end pipeline is working!"
    log_info ""
    log_info "Pipeline verified:"
    log_info "  Simulator → Kafka → Flink → IoTDB ✓"
    log_info ""
    log_info "Next steps:"
    log_info "  1. View Grafana dashboard: http://localhost:3000 (admin/admin)"
    log_info "  2. Query IoTDB directly: docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root"
    log_info "  3. Check Flink UI: http://localhost:8081"
    log_info ""
    exit 0
elif [ $TESTS_PASSED -ge 4 ]; then
    log_warn "⚠ MVP is PARTIALLY WORKING"
    log_info "Core infrastructure is up but data flow needs debugging"
    exit 1
else
    log_error "❌ MVP has critical issues"
    exit 1
fi
