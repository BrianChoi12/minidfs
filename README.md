# MiniDFS - A Distributed File System

A lightweight, educational distributed file system implementation written in C++ with gRPC, featuring chunked file storage, metadata management, and comprehensive testing.

## Overview

MiniDFS is a simple yet functional distributed file system that demonstrates core concepts of distributed storage systems. Files are automatically chunked (1MB chunks), distributed across DataNodes, and managed by a centralized MetaServer for metadata coordination.

### Architecture

```
Client ←→ MetaServer ←→ DataNode(s)
          (Metadata)     (File Chunks)
```

- **Client**: Handles file upload/download operations with automatic chunking
- **MetaServer**: Manages file metadata, chunk locations, and DataNode coordination  
- **DataNode**: Provides persistent chunk storage with integrity checking (SHA-256)

## Features

- ✅ **Chunked File Storage**: Automatic 1MB chunking for efficient distribution
- ✅ **Metadata Management**: Centralized file location and chunk mapping
- ✅ **Data Integrity**: SHA-256 checksums for all stored chunks
- ✅ **Load Balancing**: Intelligent chunk placement based on DataNode capacity and load
- ✅ **Empty File Support**: Proper handling of 0-byte files
- ✅ **Binary File Support**: Complete binary data preservation
- ✅ **Concurrent Operations**: Thread-safe operations across all components
- ✅ **Comprehensive Testing**: Unit, integration, e2e, and performance tests

## Getting Started

### Prerequisites

- CMake 3.16+
- C++17 compiler
- gRPC and Protocol Buffers
- OpenSSL (for checksums)
- Google Test (for testing)

### Building

```bash
mkdir build && cd build
cmake ..
make
```

### Running the System

1. **Start MetaServer**:
```bash
./minidfs_metaserver
```

2. **Start DataNode**:
```bash
./minidfs_datanode
```

3. **Upload/Download Files**:
```bash
./minidfs_client upload /path/to/file
./minidfs_client download filename
```

## Testing

The project includes comprehensive test coverage:

```bash
# Run all tests
make test_all

# Run specific test suites
make test_unit        # Unit tests (cache, storage)
make test_integration # Integration tests (MetaServer)
make test_e2e        # End-to-end system tests
make test_performance # Performance benchmarks
```

### Test Results
- **Unit Tests**: ✅ All passing (cache, storage components)
- **Integration Tests**: ✅ All passing (MetaServer functionality) 
- **E2E Tests**: ✅ 15/16 passing (93.75% success rate)
- **Performance Tests**: ✅ All passing (throughput, stability)

## Project Structure

```
minidfs/
├── proto/              # Protocol Buffer definitions
├── client/             # Client implementation
├── metaserver/         # MetaServer with Manager and Cache
├── datanode/           # DataNode with persistent storage
├── common/             # Shared utilities
├── tests/              # Comprehensive test suite
│   ├── unit/          # Component unit tests
│   ├── integration/   # Service integration tests
│   ├── e2e/           # End-to-end system tests
│   └── utils/         # Test utilities and fixtures
└── build/              # Build artifacts
```

## Implementation Highlights

### Chunk Storage Strategy
- Files automatically split into 1MB chunks
- Unique server-generated chunk IDs prevent collisions
- Distributed storage with configurable replication (currently 1x)

### MetaServer Design
- **Manager**: Handles chunk allocation and DataNode selection
- **Cache**: LRU cache for frequently accessed chunk locations
- **Thread Safety**: All operations are thread-safe with proper locking

### DataNode Features  
- **File-based Storage**: Chunks stored as individual files with subdirectories
- **Integrity Checking**: SHA-256 checksums for all chunks
- **Health Monitoring**: Periodic health checks and stale node cleanup
- **Load Tracking**: Current load monitoring for optimal allocation

### Protocol Design
- **Trust Model**: DataNodes report chunk status via heartbeats (clients don't)
- **Per-chunk Allocation**: Dynamic chunk-by-chunk allocation during upload
- **Error Handling**: Comprehensive error handling with retry logic

## Educational Value

This project demonstrates several important distributed systems concepts:

- **Distributed Architecture**: Client-server model with multiple storage nodes
- **Consistency Models**: Metadata consistency through centralized coordination
- **Load Balancing**: Capacity and load-based chunk placement algorithms  
- **Fault Tolerance**: Health monitoring and stale node detection
- **Data Integrity**: End-to-end integrity checking with cryptographic hashes
- **Concurrent Programming**: Thread-safe operations and resource management

## Playing Around with Claude Code

This project represents my first experience using **Claude Code** for development, and it's been an incredible learning journey! 🚀

**What made this special:**
- **AI-Assisted Architecture**: Claude helped design the entire distributed system architecture, from protocol definitions to component interactions
- **Comprehensive Implementation**: We built a complete system with all three components (Client, MetaServer, DataNode) from scratch
- **Test-Driven Development**: Developed extensive test suites covering unit, integration, e2e, and performance testing
- **Problem Solving**: When tests initially failed, Claude helped debug complex issues around gRPC initialization, file path handling, and empty file edge cases
- **Code Quality**: The AI helped maintain clean, well-documented code with proper error handling throughout

**Key Learning Moments:**
- Debugging hanging tests and discovering gRPC static initialization issues
- Understanding the nuances of distributed file system design decisions
- Implementing proper empty file handling across all system components
- Creating comprehensive test infrastructure that actually works reliably

**Results**: From concept to a 93.75% passing distributed file system in one session! The AI's ability to maintain context across such a large codebase, debug complex distributed system issues, and provide detailed explanations made this an incredibly productive and educational experience.

## Performance

Current performance benchmarks (single DataNode):
- **Small Files**: ~1000 files/minute upload+download
- **Large Files**: ~19 MB/s sustained throughput  
- **Stability**: 200+ operations over 10+ seconds with 0 failures
- **Concurrency**: Successfully handles 10+ concurrent clients

## Future Improvements

- [ ] Multiple DataNode replication
- [ ] DataNode failure recovery
- [ ] Distributed MetaServer (eliminate SPOF)
- [ ] Compression support
- [ ] Web-based administration interface
- [ ] Metrics and monitoring integration

## License

This project is for educational purposes. Feel free to use, modify, and learn from it!

---

*Built with ❤️ and Claude Code - exploring the future of AI-assisted development*