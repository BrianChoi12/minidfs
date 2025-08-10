#!/bin/bash

set -e  # Exit on any command failure

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/build"

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}[$(date '+%H:%M:%S')] $message${NC}"
}

print_header() {
    echo -e "\n${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}\n"
}

# Function to check if build directory exists and build if needed
ensure_build() {
    if [ ! -d "$BUILD_DIR" ]; then
        print_status $YELLOW "Build directory not found. Creating and building project..."
        mkdir -p "$BUILD_DIR"
        cd "$BUILD_DIR"
        cmake ..
        make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    else
        cd "$BUILD_DIR"
        print_status $BLUE "Building project..."
        make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    fi
}

# Function to run a specific test suite
run_test_suite() {
    local test_name=$1
    local test_executable=$2
    local filter_pattern=${3:-"*"}
    
    print_header "Running $test_name"
    
    if [ ! -f "$test_executable" ]; then
        print_status $RED "Test executable not found: $test_executable"
        return 1
    fi
    
    # Run tests with optional filter
    if [ "$filter_pattern" = "*" ]; then
        if "$test_executable" --gtest_color=yes; then
            print_status $GREEN "$test_name: PASSED"
            return 0
        else
            print_status $RED "$test_name: FAILED"
            return 1
        fi
    else
        if "$test_executable" --gtest_filter="$filter_pattern" --gtest_color=yes; then
            print_status $GREEN "$test_name (filtered): PASSED"
            return 0
        else
            print_status $RED "$test_name (filtered): FAILED"
            return 1
        fi
    fi
}

# Function to run performance tests with detailed output
run_performance_tests() {
    print_header "Running Performance Tests"
    
    local test_executable="$BUILD_DIR/e2e_tests"
    
    if [ ! -f "$test_executable" ]; then
        print_status $RED "Performance test executable not found: $test_executable"
        return 1
    fi
    
    # Run with performance filter and verbose output
    if "$test_executable" --gtest_filter="PerformanceTest*" --gtest_color=yes; then
        print_status $GREEN "Performance Tests: COMPLETED"
        return 0
    else
        print_status $RED "Performance Tests: FAILED"
        return 1
    fi
}

# Function to generate test report
generate_report() {
    local results_file="$BUILD_DIR/test_results.xml"
    print_header "Generating Test Report"
    
    # Run all tests with XML output
    local all_passed=true
    
    if [ -f "$BUILD_DIR/unit_tests" ]; then
        if ! "$BUILD_DIR/unit_tests" --gtest_output="xml:${BUILD_DIR}/unit_test_results.xml"; then
            all_passed=false
        fi
    fi
    
    if [ -f "$BUILD_DIR/integration_tests" ]; then
        if ! "$BUILD_DIR/integration_tests" --gtest_output="xml:${BUILD_DIR}/integration_test_results.xml"; then
            all_passed=false
        fi
    fi
    
    if [ -f "$BUILD_DIR/e2e_tests" ]; then
        if ! "$BUILD_DIR/e2e_tests" --gtest_output="xml:${BUILD_DIR}/e2e_test_results.xml"; then
            all_passed=false
        fi
    fi
    
    if [ "$all_passed" = true ]; then
        print_status $GREEN "All tests passed! Reports generated in $BUILD_DIR"
    else
        print_status $RED "Some tests failed. Check reports in $BUILD_DIR"
        return 1
    fi
}

# Function to clean test artifacts
clean_test_artifacts() {
    print_status $YELLOW "Cleaning test artifacts..."
    
    # Remove temporary test files
    find /tmp -name "minidfs_test_*" -type f -delete 2>/dev/null || true
    find /tmp -name "minidfs_test_dir_*" -type d -exec rm -rf {} + 2>/dev/null || true
    
    # Clean build artifacts if requested
    if [ "$1" = "--deep" ]; then
        rm -rf "$BUILD_DIR"
        print_status $YELLOW "Build directory cleaned"
    fi
}

# Function to show help
show_help() {
    echo "MiniDFS Test Runner"
    echo ""
    echo "Usage: $0 [OPTIONS] [TEST_TYPE]"
    echo ""
    echo "TEST_TYPE:"
    echo "  unit         Run unit tests only"
    echo "  integration  Run integration tests only"
    echo "  e2e          Run end-to-end tests only"
    echo "  performance  Run performance tests only"
    echo "  all          Run all tests (default)"
    echo ""
    echo "OPTIONS:"
    echo "  --filter PATTERN    Run tests matching the pattern"
    echo "  --report            Generate XML test reports"
    echo "  --clean             Clean test artifacts before running"
    echo "  --clean-deep        Clean build directory before running"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # Run all tests"
    echo "  $0 unit                      # Run unit tests only"
    echo "  $0 --filter \"*Cache*\"        # Run tests with Cache in the name"
    echo "  $0 performance --report      # Run performance tests and generate report"
    echo "  $0 --clean all              # Clean and run all tests"
}

# Parse command line arguments
TEST_TYPE="all"
FILTER_PATTERN=""
GENERATE_REPORT=false
CLEAN_BEFORE=false
CLEAN_DEEP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --filter)
            FILTER_PATTERN="$2"
            shift 2
            ;;
        --report)
            GENERATE_REPORT=true
            shift
            ;;
        --clean)
            CLEAN_BEFORE=true
            shift
            ;;
        --clean-deep)
            CLEAN_DEEP=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        unit|integration|e2e|performance|all)
            TEST_TYPE="$1"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_header "MiniDFS Test Suite"
    print_status $BLUE "Test type: $TEST_TYPE"
    if [ -n "$FILTER_PATTERN" ]; then
        print_status $BLUE "Filter pattern: $FILTER_PATTERN"
    fi
    
    # Clean if requested
    if [ "$CLEAN_DEEP" = true ]; then
        clean_test_artifacts --deep
    elif [ "$CLEAN_BEFORE" = true ]; then
        clean_test_artifacts
    fi
    
    # Ensure project is built
    ensure_build
    
    # Track test results
    local failed_tests=0
    local total_tests=0
    
    # Run requested tests
    case $TEST_TYPE in
        unit)
            total_tests=1
            if ! run_test_suite "Unit Tests" "$BUILD_DIR/unit_tests" "$FILTER_PATTERN"; then
                ((failed_tests++))
            fi
            ;;
        integration)
            total_tests=1
            if ! run_test_suite "Integration Tests" "$BUILD_DIR/integration_tests" "$FILTER_PATTERN"; then
                ((failed_tests++))
            fi
            ;;
        e2e)
            total_tests=1
            if ! run_test_suite "End-to-End Tests" "$BUILD_DIR/e2e_tests" "$FILTER_PATTERN"; then
                ((failed_tests++))
            fi
            ;;
        performance)
            total_tests=1
            if ! run_performance_tests; then
                ((failed_tests++))
            fi
            ;;
        all)
            total_tests=3
            
            if ! run_test_suite "Unit Tests" "$BUILD_DIR/unit_tests" "$FILTER_PATTERN"; then
                ((failed_tests++))
            fi
            
            if ! run_test_suite "Integration Tests" "$BUILD_DIR/integration_tests" "$FILTER_PATTERN"; then
                ((failed_tests++))
            fi
            
            if ! run_test_suite "End-to-End Tests" "$BUILD_DIR/e2e_tests" "$FILTER_PATTERN"; then
                ((failed_tests++))
            fi
            ;;
    esac
    
    # Generate report if requested
    if [ "$GENERATE_REPORT" = true ]; then
        generate_report
    fi
    
    # Clean up temporary artifacts
    clean_test_artifacts
    
    # Print final results
    print_header "Test Results Summary"
    local passed_tests=$((total_tests - failed_tests))
    print_status $BLUE "Total test suites: $total_tests"
    print_status $GREEN "Passed: $passed_tests"
    
    if [ $failed_tests -eq 0 ]; then
        print_status $GREEN "All test suites passed! ✓"
        exit 0
    else
        print_status $RED "Failed: $failed_tests"
        print_status $RED "Some test suites failed! ✗"
        exit 1
    fi
}

# Run main function
main