#!/bin/bash
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# Test runner script for alertsgenconnector
# This script runs all tests with coverage and generates reports

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== AlertsGen Connector Test Suite ===${NC}"
echo ""

# Change to the connector directory
cd "$(dirname "$0")"

# Clean previous coverage files
rm -f coverage.txt coverage.html

# Run unit tests with coverage
echo -e "${YELLOW}Running unit tests...${NC}"
go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

# Check if tests passed
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ All unit tests passed${NC}"
else
    echo -e "${RED}✗ Unit tests failed${NC}"
    exit 1
fi

# Run integration tests (if separate)
echo ""
echo -e "${YELLOW}Running integration tests...${NC}"
go test -v -race -run Integration ./...

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ All integration tests passed${NC}"
else
    echo -e "${RED}✗ Integration tests failed${NC}"
    exit 1
fi

# Run benchmarks (optional, comment out for CI)
echo ""
echo -e "${YELLOW}Running benchmarks...${NC}"
go test -bench=. -benchmem -run=^$ ./... | grep -E "^Benchmark|^goos:|^goarch:|^cpu:|^$"

# Generate coverage report
echo ""
echo -e "${YELLOW}Generating coverage report...${NC}"
go tool cover -html=coverage.txt -o coverage.html
go tool cover -func=coverage.txt | tail -n 1

# Run go vet
echo ""
echo -e "${YELLOW}Running go vet...${NC}"
go vet ./...

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ No vet issues found${NC}"
else
    echo -e "${RED}✗ Vet issues found${NC}"
    exit 1
fi

# Run golint (if installed)
if command -v golint &> /dev/null; then
    echo ""
    echo -e "${YELLOW}Running golint...${NC}"
    golint ./...
fi

# Run staticcheck (if installed)
if command -v staticcheck &> /dev/null; then
    echo ""
    echo -e "${YELLOW}Running staticcheck...${NC}"
    staticcheck ./...
fi

# Run race detector on specific test
echo ""
echo -e "${YELLOW}Running race detector tests...${NC}"
go test -race -run TestConcurrent ./...

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ No race conditions detected${NC}"
else
    echo -e "${RED}✗ Race conditions detected${NC}"
    exit 1
fi

# Test with different configurations
echo ""
echo -e "${YELLOW}Testing with different configurations...${NC}"

# Test with minimal config
go test -v -run TestCreateDefaultConfig ./...

# Test with memory pressure
go test -v -run TestIntegrationWithMemoryPressure ./...

# Summary
echo ""
echo -e "${GREEN}=== Test Summary ===${NC}"
echo "Coverage report: coverage.html"
echo "Coverage data: coverage.txt"

# Extract coverage percentage
COVERAGE=$(go tool cover -func=coverage.txt | tail -n 1 | awk '{print $3}')
echo -e "Total coverage: ${GREEN}${COVERAGE}${NC}"

# Check if coverage meets threshold (e.g., 80%)
THRESHOLD=80
COVERAGE_NUM=$(echo $COVERAGE | sed 's/%//')
if (( $(echo "$COVERAGE_NUM >= $THRESHOLD" | bc -l) )); then
    echo -e "${GREEN}✓ Coverage meets threshold (>=${THRESHOLD}%)${NC}"
else
    echo -e "${YELLOW}⚠ Coverage below threshold (<${THRESHOLD}%)${NC}"
fi

echo ""
echo -e "${GREEN}=== All tests completed successfully! ===${NC}"