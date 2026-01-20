#!/bin/bash
#
# E2E REST API Test Script for ANN API Server
# Usage: ./e2e-test.sh [BASE_URL]
#

set -e

BASE_URL="${1:-http://localhost:8080}"
API_V1="${BASE_URL}/api/v1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0

# Helper functions
log_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED++))
}

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Check if response contains expected value
assert_contains() {
    local response="$1"
    local expected="$2"
    local test_name="$3"

    if echo "$response" | grep -q "$expected"; then
        log_pass "$test_name"
        return 0
    else
        log_fail "$test_name - Expected to contain: $expected"
        echo "  Response: $response"
        return 1
    fi
}

# Check HTTP status code
assert_status() {
    local actual="$1"
    local expected="$2"
    local test_name="$3"

    if [ "$actual" == "$expected" ]; then
        log_pass "$test_name (HTTP $actual)"
        return 0
    else
        log_fail "$test_name - Expected HTTP $expected, got HTTP $actual"
        return 1
    fi
}

echo "=================================================="
echo "   ANN API Server E2E Test Suite"
echo "   Base URL: $BASE_URL"
echo "=================================================="
echo ""

# ==================================================
# 1. HEALTH CHECK TESTS
# ==================================================
echo -e "${YELLOW}=== Health Check Tests ===${NC}"

# Test 1.1: GET /api/v1/health
log_test "GET /api/v1/health - Basic health check"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/health")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Health endpoint returns 200"
assert_contains "$BODY" '"status":"healthy"' "Health status is healthy"
assert_contains "$BODY" '"version":"1.0.0"' "Version is 1.0.0"

# Test 1.2: GET /api/v1/health/ready
log_test "GET /api/v1/health/ready - Readiness probe"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/health/ready")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Readiness endpoint returns 200"
assert_contains "$BODY" '"ready":true' "Service is ready"

# Test 1.3: GET /api/v1/health/live
log_test "GET /api/v1/health/live - Liveness probe"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/health/live")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Liveness endpoint returns 200"
assert_contains "$BODY" '"alive":true' "Service is alive"

echo ""

# ==================================================
# 2. INDEX MANAGEMENT TESTS
# ==================================================
echo -e "${YELLOW}=== Index Management Tests ===${NC}"

# Test 2.1: GET /api/v1/indexes - List indexes (initially empty)
log_test "GET /api/v1/indexes - List indexes (empty)"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "List indexes returns 200"
assert_contains "$BODY" '"indexes":[]' "Indexes list is empty"

# Test 2.2: POST /api/v1/indexes - Create index with vectors
log_test "POST /api/v1/indexes - Create index 'test-index-1'"
CREATE_REQUEST='{
  "indexId": "test-index-1",
  "vectors": [
    {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4]},
    {"id": 2, "vector": [0.2, 0.3, 0.4, 0.5]},
    {"id": 3, "vector": [0.3, 0.4, 0.5, 0.6]},
    {"id": 4, "vector": [0.4, 0.5, 0.6, 0.7]},
    {"id": 5, "vector": [0.5, 0.6, 0.7, 0.8]}
  ],
  "config": {
    "m": 16,
    "efConstruction": 100,
    "distanceType": "euclidean"
  }
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes" \
    -H "Content-Type: application/json" \
    -d "$CREATE_REQUEST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "201" "Create index returns 201 Created"
assert_contains "$BODY" '"success":true' "Index creation successful"
assert_contains "$BODY" '"indexId":"test-index-1"' "Index ID matches"

# Test 2.3: POST /api/v1/indexes - Create second index
log_test "POST /api/v1/indexes - Create index 'test-index-2'"
CREATE_REQUEST2='{
  "indexId": "test-index-2",
  "vectors": [
    {"id": 101, "vector": [0.9, 0.8, 0.7, 0.6]},
    {"id": 102, "vector": [0.8, 0.7, 0.6, 0.5]},
    {"id": 103, "vector": [0.7, 0.6, 0.5, 0.4]}
  ]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes" \
    -H "Content-Type: application/json" \
    -d "$CREATE_REQUEST2")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "201" "Create second index returns 201 Created"
assert_contains "$BODY" '"success":true' "Second index creation successful"

# Test 2.4: POST /api/v1/indexes - Duplicate index (should fail)
log_test "POST /api/v1/indexes - Create duplicate index (expect failure)"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes" \
    -H "Content-Type: application/json" \
    -d "$CREATE_REQUEST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "409" "Duplicate index returns 409 Conflict"

# Test 2.5: GET /api/v1/indexes - List indexes (should have 2)
log_test "GET /api/v1/indexes - List indexes (should have 2)"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "List indexes returns 200"
assert_contains "$BODY" '"totalIndexes":2' "Total indexes is 2"

# Test 2.6: GET /api/v1/indexes/{indexId} - Get index info
log_test "GET /api/v1/indexes/test-index-1 - Get index info"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes/test-index-1")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Get index info returns 200"
assert_contains "$BODY" '"indexId":"test-index-1"' "Index ID matches"
assert_contains "$BODY" '"dimension":4' "Dimension is 4"
assert_contains "$BODY" '"size":5' "Size is 5 vectors"

# Test 2.7: GET /api/v1/indexes/{indexId} - Non-existent index
log_test "GET /api/v1/indexes/non-existent - Non-existent index"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes/non-existent")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "404" "Non-existent index returns 404"

# Test 2.8: POST /api/v1/indexes/{indexId}/vectors - Add vectors
log_test "POST /api/v1/indexes/test-index-1/vectors - Add more vectors"
ADD_VECTORS='{
  "vectors": [
    {"id": 6, "vector": [0.6, 0.7, 0.8, 0.9]},
    {"id": 7, "vector": [0.7, 0.8, 0.9, 1.0]}
  ]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/vectors" \
    -H "Content-Type: application/json" \
    -d "$ADD_VECTORS")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Add vectors returns 200"
assert_contains "$BODY" '"success":true' "Add vectors successful"

# Test 2.9: Verify vectors were added
log_test "GET /api/v1/indexes/test-index-1 - Verify vectors added"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes/test-index-1")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_contains "$BODY" '"size":7' "Size is now 7 vectors"

echo ""

# ==================================================
# 3. SEARCH TESTS
# ==================================================
echo -e "${YELLOW}=== Search Tests ===${NC}"

# Test 3.1: POST /api/v1/indexes/{indexId}/search - Basic search
log_test "POST /api/v1/indexes/test-index-1/search - Basic search"
SEARCH_REQUEST='{
  "vector": [0.15, 0.25, 0.35, 0.45],
  "k": 3
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/search" \
    -H "Content-Type: application/json" \
    -d "$SEARCH_REQUEST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Search returns 200"
assert_contains "$BODY" '"indexId":"test-index-1"' "Index ID in response"
assert_contains "$BODY" '"results"' "Results array present"
assert_contains "$BODY" '"queryTimeMs"' "Query time present"

# Test 3.2: POST /api/v1/indexes/{indexId}/search - Search with ef parameter
log_test "POST /api/v1/indexes/test-index-1/search - Search with ef parameter"
SEARCH_REQUEST_EF='{
  "vector": [0.15, 0.25, 0.35, 0.45],
  "k": 5,
  "ef": 100
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/search" \
    -H "Content-Type: application/json" \
    -d "$SEARCH_REQUEST_EF")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Search with ef returns 200"

# Test 3.3: Search non-existent index
log_test "POST /api/v1/indexes/non-existent/search - Non-existent index"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/non-existent/search" \
    -H "Content-Type: application/json" \
    -d "$SEARCH_REQUEST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "404" "Search non-existent index returns 404"

# Test 3.4: Search with dimension mismatch
log_test "POST /api/v1/indexes/test-index-1/search - Dimension mismatch"
WRONG_DIM_REQUEST='{
  "vector": [0.1, 0.2, 0.3],
  "k": 3
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/search" \
    -H "Content-Type: application/json" \
    -d "$WRONG_DIM_REQUEST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "422" "Dimension mismatch returns 422"

# Test 3.5: Search with invalid k (k=0)
log_test "POST /api/v1/indexes/test-index-1/search - Invalid k=0"
INVALID_K_REQUEST='{
  "vector": [0.1, 0.2, 0.3, 0.4],
  "k": 0
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/search" \
    -H "Content-Type: application/json" \
    -d "$INVALID_K_REQUEST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "400" "Invalid k=0 returns 400"

# Test 3.6: Search with invalid k (k>1000)
log_test "POST /api/v1/indexes/test-index-1/search - Invalid k>1000"
INVALID_K_LARGE='{
  "vector": [0.1, 0.2, 0.3, 0.4],
  "k": 1001
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/search" \
    -H "Content-Type: application/json" \
    -d "$INVALID_K_LARGE")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "400" "k>1000 returns 400"

# Test 3.7: Search with empty vector
log_test "POST /api/v1/indexes/test-index-1/search - Empty vector"
EMPTY_VECTOR='{
  "vector": [],
  "k": 3
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/indexes/test-index-1/search" \
    -H "Content-Type: application/json" \
    -d "$EMPTY_VECTOR")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "400" "Empty vector returns 400"

echo ""

# ==================================================
# 4. MULTI-INDEX SEARCH TESTS
# ==================================================
echo -e "${YELLOW}=== Multi-Index Search Tests ===${NC}"

# Test 4.1: POST /api/v1/search - Search all indexes
log_test "POST /api/v1/search - Multi-index search (all indexes)"
MULTI_SEARCH='{
  "vector": [0.5, 0.5, 0.5, 0.5],
  "k": 3
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/search" \
    -H "Content-Type: application/json" \
    -d "$MULTI_SEARCH")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Multi-search returns 200"
assert_contains "$BODY" '"results"' "Results object present"
assert_contains "$BODY" '"merged"' "Merged results present"
assert_contains "$BODY" '"totalTimeMs"' "Total time present"

# Test 4.2: POST /api/v1/search - Search specific indexes
log_test "POST /api/v1/search - Multi-search specific indexes"
MULTI_SEARCH_SPECIFIC='{
  "vector": [0.5, 0.5, 0.5, 0.5],
  "k": 2,
  "indexIds": ["test-index-1"]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/search" \
    -H "Content-Type: application/json" \
    -d "$MULTI_SEARCH_SPECIFIC")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Multi-search specific indexes returns 200"
assert_contains "$BODY" 'test-index-1' "Results contain test-index-1"

# Test 4.3: POST /api/v1/search - Search with non-existent index in list
log_test "POST /api/v1/search - Non-existent index in list"
MULTI_SEARCH_NONEXIST='{
  "vector": [0.5, 0.5, 0.5, 0.5],
  "k": 2,
  "indexIds": ["test-index-1", "non-existent"]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/search" \
    -H "Content-Type: application/json" \
    -d "$MULTI_SEARCH_NONEXIST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "404" "Non-existent index in list returns 404"

echo ""

# ==================================================
# 5. BATCH SEARCH TESTS
# ==================================================
echo -e "${YELLOW}=== Batch Search Tests ===${NC}"

# Test 5.1: POST /api/v1/search/batch - Batch search
log_test "POST /api/v1/search/batch - Batch search with multiple queries"
BATCH_SEARCH='{
  "indexId": "test-index-1",
  "queries": [
    {"vector": [0.1, 0.2, 0.3, 0.4], "k": 2},
    {"vector": [0.5, 0.6, 0.7, 0.8], "k": 3},
    {"vector": [0.9, 0.8, 0.7, 0.6], "k": 2}
  ],
  "ef": 50
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/search/batch" \
    -H "Content-Type: application/json" \
    -d "$BATCH_SEARCH")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Batch search returns 200"
assert_contains "$BODY" '"results"' "Results array present"
assert_contains "$BODY" '"queryIndex"' "Query index present"
assert_contains "$BODY" '"totalTimeMs"' "Total time present"

# Test 5.2: Batch search with non-existent index
log_test "POST /api/v1/search/batch - Non-existent index"
BATCH_NONEXIST='{
  "indexId": "non-existent",
  "queries": [
    {"vector": [0.1, 0.2, 0.3, 0.4], "k": 2}
  ]
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/search/batch" \
    -H "Content-Type: application/json" \
    -d "$BATCH_NONEXIST")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "404" "Batch search non-existent index returns 404"

# Test 5.3: Batch search with empty queries
log_test "POST /api/v1/search/batch - Empty queries"
BATCH_EMPTY='{
  "indexId": "test-index-1",
  "queries": []
}'
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${API_V1}/search/batch" \
    -H "Content-Type: application/json" \
    -d "$BATCH_EMPTY")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "400" "Empty batch queries returns 400"

echo ""

# ==================================================
# 6. INDEX DELETION TESTS
# ==================================================
echo -e "${YELLOW}=== Index Deletion Tests ===${NC}"

# Test 6.1: DELETE /api/v1/indexes/{indexId} - Delete index
log_test "DELETE /api/v1/indexes/test-index-2 - Delete index"
RESPONSE=$(curl -s -w "\n%{http_code}" -X DELETE "${API_V1}/indexes/test-index-2")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_status "$HTTP_CODE" "200" "Delete index returns 200"
assert_contains "$BODY" '"success":true' "Delete successful"

# Test 6.2: Verify index was deleted
log_test "GET /api/v1/indexes/test-index-2 - Verify deletion"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes/test-index-2")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "404" "Deleted index returns 404"

# Test 6.3: DELETE non-existent index
log_test "DELETE /api/v1/indexes/non-existent - Non-existent index"
RESPONSE=$(curl -s -w "\n%{http_code}" -X DELETE "${API_V1}/indexes/non-existent")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
assert_status "$HTTP_CODE" "404" "Delete non-existent index returns 404"

# Test 6.4: Verify only one index remains
log_test "GET /api/v1/indexes - Verify one index remains"
RESPONSE=$(curl -s -w "\n%{http_code}" "${API_V1}/indexes")
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')
assert_contains "$BODY" '"totalIndexes":1' "Only 1 index remains"

echo ""

# ==================================================
# CLEANUP - Delete remaining test indexes
# ==================================================
echo -e "${YELLOW}=== Cleanup ===${NC}"
log_info "Cleaning up test indexes..."
curl -s -X DELETE "${API_V1}/indexes/test-index-1" > /dev/null 2>&1 || true

# ==================================================
# SUMMARY
# ==================================================
echo ""
echo "=================================================="
echo "   Test Summary"
echo "=================================================="
TOTAL=$((PASSED + FAILED))
echo -e "   Total Tests: $TOTAL"
echo -e "   ${GREEN}Passed: $PASSED${NC}"
echo -e "   ${RED}Failed: $FAILED${NC}"
echo "=================================================="

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi
