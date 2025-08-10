#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "dfs.grpc.pb.h"
#include "cache.hpp"

namespace test_utils {

// File utilities
class TempFile {
private:
    std::string path_;
    bool cleanup_on_destroy_;
    
public:
    explicit TempFile(const std::string& content = "", bool cleanup = true);
    ~TempFile();
    
    const std::string& path() const { return path_; }
    void write(const std::string& content);
    std::string read() const;
    size_t size() const;
    void cleanup();
};

class TempDirectory {
private:
    std::string path_;
    bool cleanup_on_destroy_;
    
public:
    explicit TempDirectory(bool cleanup = true);
    ~TempDirectory();
    
    const std::string& path() const { return path_; }
    std::string file_path(const std::string& filename) const;
    void cleanup();
};

// Data generation utilities
std::vector<char> generateRandomData(size_t size);
std::string generateRandomString(size_t length);
std::vector<char> generateZeroData(size_t size);
std::vector<char> generatePatternData(size_t size, const std::string& pattern);

// Server management utilities
class TestServer {
public:
    virtual ~TestServer() = default;
    virtual bool start() = 0;
    virtual void stop() = 0;
    virtual bool isRunning() const = 0;
    virtual std::string address() const = 0;
};

// Forward declare for TestMetaServer
class TestRPCServiceImpl;

class TestMetaServer : public TestServer {
private:
    std::unique_ptr<grpc::Server> server_;
    std::thread server_thread_;
    std::string address_;
    std::atomic<bool> running_{false};
    std::unique_ptr<TestRPCServiceImpl> service_;
    
public:
    explicit TestMetaServer(const std::string& address = "localhost:0");
    ~TestMetaServer();
    
    bool start() override;
    void stop() override;
    bool isRunning() const override;
    std::string address() const override;
};

// Forward declare for TestDataNode
class TestDataNodeServiceImpl;

class TestDataNode : public TestServer {
private:
    std::unique_ptr<grpc::Server> server_;
    std::thread server_thread_;
    std::thread heartbeat_thread_;
    std::string address_;
    std::string metaserver_addr_;
    std::string storage_path_;
    std::atomic<bool> running_{false};
    std::unique_ptr<TestDataNodeServiceImpl> service_;
    
public:
    TestDataNode(const std::string& address = "localhost:0",
                const std::string& metaserver_addr = "localhost:50051",
                const std::string& storage_path = "");
    ~TestDataNode();
    
    bool start() override;
    void stop() override;
    bool isRunning() const override;
    std::string address() const override;
    std::string storagePath() const { return storage_path_; }
};

// Client utilities
class TestClient {
private:
    std::unique_ptr<MetaService::Stub> stub_;
    
public:
    explicit TestClient(const std::string& metaserver_addr);
    
    // High-level operations
    bool uploadFile(const std::string& filename, const std::vector<char>& data);
    std::vector<char> downloadFile(const std::string& filename);
    
    // Low-level operations
    bool allocateChunk(const std::string& filename, int chunk_index, 
                      int64_t chunk_size, std::string& chunk_id, 
                      std::vector<std::string>& datanode_addrs);
    std::vector<ChunkLocation> getFileLocation(const std::string& filename);
};

// Assertion utilities
void expectFilesEqual(const std::string& file1, const std::string& file2);
void expectDataEqual(const std::vector<char>& data1, const std::vector<char>& data2);
void expectChunkExists(const std::string& storage_path, const std::string& chunk_id);

// Timing utilities
class Timer {
private:
    std::chrono::high_resolution_clock::time_point start_time_;
    
public:
    Timer();
    void reset();
    double elapsedSeconds() const;
    int64_t elapsedMilliseconds() const;
};

// Wait utilities
bool waitForCondition(std::function<bool()> condition, 
                     std::chrono::milliseconds timeout = std::chrono::milliseconds(5000),
                     std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100));

bool waitForServerReady(const std::string& address, 
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

// Network utilities
int findAvailablePort();
std::string createTestAddress();

} // namespace test_utils