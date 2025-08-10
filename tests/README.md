# MiniDFS Test Suite

This directory contains comprehensive tests for the MiniDFS distributed file system.

## Test Organization

### 1. **Unit Tests** (`unit/`)
Test individual components in isolation:
- `cache_test.cpp`: LRU cache functionality and thread safety
- `storage_test.cpp`: DataNode storage operations, persistence, and integrity

### 2. **Integration Tests** (`integration/`)
Test component interactions:
- `metaserver_test.cpp`: MetaServer RPC services, DataNode management, and load balancing

### 3. **End-to-End Tests** (`e2e/`)
Test complete system workflows:
- `full_system_test.cpp`: Complete upload/download cycles, multi-file operations, data integrity
- `performance_test.cpp`: Throughput, latency, stress testing, and system stability

### 4. **Test Utilities** (`utils/`)
Common testing infrastructure:
- `test_utils.hpp/.cpp`: File utilities, server management, client helpers, assertion utilities

## Running Tests

### Quick Start
```bash
# Run all tests
./scripts/run_tests.sh

# Run specific test suite
./scripts/run_tests.sh unit
./scripts/run_tests.sh integration
./scripts/run_tests.sh e2e
./scripts/run_tests.sh performance

# Run with filters
./scripts/run_tests.sh --filter "*Cache*"
./scripts/run_tests.sh unit --filter "*ThreadSafety*"

# Generate test reports
./scripts/run_tests.sh --report
```

### CMake Targets
```bash
# Build and run all tests
make test_all

# Run specific test suites
make test_unit
make test_integration
make test_e2e
make test_performance

# Build test executables
make unit_tests
make integration_tests
make e2e_tests
```

### Manual Execution
```bash
cd build

# Run unit tests with specific filters
./unit_tests --gtest_filter="CacheTest*" --gtest_color=yes

# Run integration tests with verbose output
./integration_tests --gtest_color=yes --gtest_verbose

# Run performance tests
./e2e_tests --gtest_filter="PerformanceTest*"

# Generate XML reports
./unit_tests --gtest_output="xml:unit_results.xml"
```

## Test Categories

### Functional Tests
- **File Operations**: Upload, download, overwrite, empty files, binary files
- **Chunking**: Single chunk, multiple chunks, boundary conditions
- **Data Integrity**: Checksums, corruption detection, round-trip verification
- **Error Handling**: Non-existent files, capacity limits, network failures

### System Tests  
- **Multi-Component**: MetaServer + DataNode + Client interactions
- **Persistence**: Data survival across restarts
- **Load Balancing**: Chunk distribution across DataNodes
- **Heartbeat**: DataNode registration and health monitoring

### Performance Tests
- **Throughput**: Small files, large files, mixed workloads  
- **Latency**: Operation response times
- **Concurrency**: Multiple clients, parallel operations
- **Stress**: Extended operation periods, resource usage patterns
- **Scalability**: Increasing data sizes, chunk boundary behavior

### Reliability Tests
- **Data Integrity**: Checksum verification, corruption detection
- **System Stability**: Long-running operations, error recovery
- **Thread Safety**: Concurrent access to shared resources

## Test Infrastructure

### Server Management
- **TestMetaServer**: Automated MetaServer lifecycle management
- **TestDataNode**: Automated DataNode setup with temporary storage
- **Port Management**: Dynamic port allocation to prevent conflicts

### File Management  
- **TempFile**: Automatic temporary file creation and cleanup
- **TempDirectory**: Temporary directory management for test isolation
- **Data Generators**: Random, pattern, and zero data generation

### Utilities
- **Timer**: Performance measurement utilities
- **Assertions**: File comparison, data verification
- **Wait Functions**: Condition polling, server readiness checks

## Configuration

### Dependencies
- **Google Test**: C++ testing framework
- **gRPC**: For client-server communication testing
- **OpenSSL**: For checksum verification in tests

### Environment Variables
- `MINIDFS_TEST_TIMEOUT`: Test timeout in seconds (default: 30)
- `MINIDFS_TEST_VERBOSE`: Enable verbose logging (default: false)
- `MINIDFS_TEST_KEEP_FILES`: Keep temporary files for debugging (default: false)

## Continuous Integration

### Test Stages
1. **Fast Tests**: Unit tests (< 30 seconds)
2. **Integration Tests**: Component interaction tests (< 2 minutes)  
3. **System Tests**: Full end-to-end tests (< 5 minutes)
4. **Performance Tests**: Throughput and stress tests (< 10 minutes)

### Coverage Requirements
- **Unit Tests**: > 80% line coverage for core components
- **Integration Tests**: All RPC endpoints covered
- **E2E Tests**: All user workflows covered

### Performance Baselines
- **Small File Throughput**: > 10 ops/sec
- **Large File Throughput**: > 0.5 MB/sec
- **Upload Latency**: < 1000ms for small files
- **Memory Usage**: Linear scaling with data size
- **System Stability**: < 5% failure rate under stress

## Debugging Tests

### Verbose Output
```bash
# Enable verbose logging
export MINIDFS_TEST_VERBOSE=1
./scripts/run_tests.sh unit

# Keep temporary files for inspection  
export MINIDFS_TEST_KEEP_FILES=1
./unit_tests --gtest_filter="StorageTest.PersistenceAcrossInstances"
```

### Manual Server Testing
```bash
# Start servers manually for debugging
cd build
./minidfs_metaserver &
./minidfs_datanode --storage-path /tmp/debug_storage &

# Run client tests against manual servers
./integration_tests --gtest_filter="*ClientServer*"
```

### Common Issues

1. **Port Conflicts**: Tests use dynamic port allocation, but conflicts can still occur
   - Solution: Run tests sequentially or increase port range

2. **Temporary Directory Cleanup**: Tests create temporary files/directories
   - Solution: Check `/tmp/minidfs_test_*` for leftover files

3. **Server Startup Timing**: Integration tests may fail if servers don't start quickly enough
   - Solution: Increase timeout values in test configuration

4. **Performance Variability**: Performance tests may be sensitive to system load
   - Solution: Run on dedicated test systems or adjust thresholds

## Adding New Tests

### Unit Test Example
```cpp
TEST_F(MyComponentTest, SpecificFeature) {
    // Arrange
    auto input = createTestInput();
    
    // Act  
    auto result = component_->processInput(input);
    
    // Assert
    EXPECT_EQ(result.status, SUCCESS);
    EXPECT_GT(result.data.size(), 0);
}
```

### Integration Test Example  
```cpp
TEST_F(ServerIntegrationTest, ClientServerInteraction) {
    // Start test servers
    ASSERT_TRUE(server_->start());
    
    // Create client
    TestClient client(server_->address());
    
    // Test interaction
    EXPECT_TRUE(client.performOperation("test_data"));
}
```

### Performance Test Example
```cpp
TEST_F(PerformanceTest, ThroughputMeasurement) {
    Timer timer;
    size_t bytes_processed = 0;
    
    // Perform operations
    for (int i = 0; i < num_operations; ++i) {
        bytes_processed += performOperation();
    }
    
    // Measure and assert
    double throughput = bytes_processed / timer.elapsedSeconds();
    EXPECT_GT(throughput, minimum_throughput);
}