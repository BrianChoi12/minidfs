# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MiniDFS is a distributed file system in C++ using gRPC. It follows a master-slave architecture with three components:
- **MetaServer**: Central metadata server managing file metadata and chunk locations (port 50051)
- **DataNode**: Storage nodes holding actual file chunks
- **Client**: CLI for file operations (upload/download)

## Build Commands

```bash
# Build the project
mkdir -p build && cd build
cmake ..
make

# Run components (from build directory)
./minidfs_metaserver  # Start metadata server
./minidfs_datanode    # Start storage node
./minidfs_client      # Start client CLI
```

## Architecture

### Component Communication
- All components communicate via gRPC using protocols defined in `proto/dfs.proto`
- MetaServer coordinates chunk locations and DataNode registrations
- Client chunks files (1MB chunks) and coordinates with MetaServer for storage locations
- DataNodes store/retrieve actual chunk data

### Key Files
- `proto/dfs.proto`: gRPC service definitions and message types
- `metaserver/rpc_service_impl.cpp`: MetaServer RPC handlers (currently stubs)
- `client/client.cpp`: Client implementation with chunking logic
- `CMakeLists.txt`: Build configuration

### Current Implementation Status
- gRPC service skeletons are complete but return empty responses
- Client has file chunking logic but incomplete upload/download
- DataNode has no implementation (empty main)
- No data persistence, error handling, or tests

## Development Notes

### When implementing features:
1. Update the corresponding RPC service implementation
2. Follow existing gRPC patterns with Status returns
3. Use 1MB chunk size for file operations
4. Generated protobuf code is in `build/generated/`

### Common tasks:
- To add a new RPC: Update `proto/dfs.proto`, rebuild, implement in service class
- To test client-server communication: Start MetaServer first, then Client
- File chunking is handled in `client/client.cpp` using 1MB buffers