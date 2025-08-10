#include "test_utils.hpp"
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "cache.hpp"
#include "manager.hpp"

namespace test_utils {

// TempFile implementation
TempFile::TempFile(const std::string& content, bool cleanup) 
    : cleanup_on_destroy_(cleanup) {
    char temp_template[] = "/tmp/minidfs_test_XXXXXX";
    int fd = mkstemp(temp_template);
    if (fd == -1) {
        throw std::runtime_error("Failed to create temporary file");
    }
    path_ = temp_template;
    close(fd);
    
    if (!content.empty()) {
        write(content);
    }
}

TempFile::~TempFile() {
    if (cleanup_on_destroy_) {
        cleanup();
    }
}

void TempFile::write(const std::string& content) {
    std::ofstream file(path_, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open temp file for writing");
    }
    file.write(content.data(), content.size());
    file.close();
}

std::string TempFile::read() const {
    std::ifstream file(path_, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open temp file for reading");
    }
    
    std::ostringstream ss;
    ss << file.rdbuf();
    return ss.str();
}

size_t TempFile::size() const {
    return std::filesystem::file_size(path_);
}

void TempFile::cleanup() {
    if (std::filesystem::exists(path_)) {
        std::filesystem::remove(path_);
    }
}

// TempDirectory implementation
TempDirectory::TempDirectory(bool cleanup) : cleanup_on_destroy_(cleanup) {
    char temp_template[] = "/tmp/minidfs_test_dir_XXXXXX";
    if (mkdtemp(temp_template) == nullptr) {
        throw std::runtime_error("Failed to create temporary directory");
    }
    path_ = temp_template;
}

TempDirectory::~TempDirectory() {
    if (cleanup_on_destroy_) {
        cleanup();
    }
}

std::string TempDirectory::file_path(const std::string& filename) const {
    return path_ + "/" + filename;
}

void TempDirectory::cleanup() {
    if (std::filesystem::exists(path_)) {
        std::filesystem::remove_all(path_);
    }
}

// Data generation utilities
std::vector<char> generateRandomData(size_t size) {
    std::vector<char> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>(dis(gen));
    }
    
    return data;
}

std::string generateRandomString(size_t length) {
    std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, chars.size() - 1);
    
    std::string result;
    result.reserve(length);
    
    for (size_t i = 0; i < length; ++i) {
        result += chars[dis(gen)];
    }
    
    return result;
}

std::vector<char> generateZeroData(size_t size) {
    return std::vector<char>(size, 0);
}

std::vector<char> generatePatternData(size_t size, const std::string& pattern) {
    std::vector<char> data;
    data.reserve(size);
    
    size_t pattern_pos = 0;
    for (size_t i = 0; i < size; ++i) {
        data.push_back(pattern[pattern_pos]);
        pattern_pos = (pattern_pos + 1) % pattern.size();
    }
    
    return data;
}

// TestMetaServer implementation
class TestRPCServiceImpl final : public MetaService::Service {
private:
    Manager* manager_;
    
public:
    explicit TestRPCServiceImpl(Manager* manager) : manager_(manager) {}
    
    grpc::Status RegisterDataNode(grpc::ServerContext* context, const ::DataNodeInfo* request, ::Ack* response) override {
        bool success = manager_->registerDataNode(request->address(), request->available_space());
        response->set_ok(success);
        response->set_message(success ? "DataNode registered successfully" : "Failed to register DataNode");
        return grpc::Status::OK;
    }
    
    grpc::Status Heartbeat(grpc::ServerContext* context, const ::DataNodeHeartbeat* request, ::HeartbeatResponse* response) override {
        std::vector<std::string> chunks;
        for (const auto& chunk : request->stored_chunk_ids()) {
            chunks.push_back(chunk);
        }
        
        bool success = manager_->updateDataNodeHeartbeat(
            request->address(), chunks, request->available_space(), request->current_load());
        
        response->set_ok(success);
        return grpc::Status::OK;
    }
    
    grpc::Status GetFileLocation(grpc::ServerContext* context, const ::FileLocationRequest* request, ::FileLocationResponse* response) override {
        auto locations = manager_->getFileLocation(request->filename());
        
        if (locations.empty()) {
            response->set_found(false);
            return grpc::Status::OK;
        }
        
        response->set_found(true);
        for (const auto& loc : locations) {
            auto* chunk_loc = response->add_chunks();
            chunk_loc->set_chunk_id(loc.chunk_id);
            for (const auto& addr : loc.datanode_addresses) {
                chunk_loc->add_datanode_addresses(addr);
            }
        }
        
        return grpc::Status::OK;
    }
    
    grpc::Status AllocateChunkLocation(grpc::ServerContext* context, const ::ChunkAllocationRequest* request, ::ChunkLocation* response) override {
        auto [chunk_id, datanode_addresses] = manager_->allocateChunkLocation(
            request->filename(), request->chunk_index(), request->chunk_size());
        
        if (chunk_id.empty()) {
            return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "No available DataNode");
        }
        
        response->set_chunk_id(chunk_id);
        for (const auto& addr : datanode_addresses) {
            response->add_datanode_addresses(addr);
        }
        
        return grpc::Status::OK;
    }
};

TestMetaServer::TestMetaServer(const std::string& address) : address_(address) {
    if (address_.find(":0") != std::string::npos) {
        // Replace :0 with available port
        int port = findAvailablePort();
        size_t colon_pos = address_.rfind(':');
        address_ = address_.substr(0, colon_pos + 1) + std::to_string(port);
    }
}

TestMetaServer::~TestMetaServer() {
    stop();
}

bool TestMetaServer::start() {
    if (running_) return true;
    
    try {
        // Create cache and manager
        static Cache cache(1000);
        static Manager manager(&cache);
        static TestRPCServiceImpl service(&manager);
        
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        
        server_ = builder.BuildAndStart();
        if (!server_) {
            return false;
        }
        
        running_ = true;
        
        server_thread_ = std::thread([this]() {
            server_->Wait();
        });
        
        return waitForServerReady(address_);
    } catch (const std::exception& e) {
        std::cerr << "Failed to start TestMetaServer: " << e.what() << std::endl;
        return false;
    }
}

void TestMetaServer::stop() {
    if (!running_) return;
    
    running_ = false;
    
    if (server_) {
        server_->Shutdown();
    }
    
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    
    server_.reset();
}

bool TestMetaServer::isRunning() const {
    return running_;
}

std::string TestMetaServer::address() const {
    return address_;
}

// Network utilities
int findAvailablePort() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return 0;
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = 0;
    
    if (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return 0;
    }
    
    socklen_t len = sizeof(addr);
    if (getsockname(sock, (struct sockaddr*)&addr, &len) < 0) {
        close(sock);
        return 0;
    }
    
    int port = ntohs(addr.sin_port);
    close(sock);
    return port;
}

std::string createTestAddress() {
    return "localhost:" + std::to_string(findAvailablePort());
}

// TestClient implementation
TestClient::TestClient(const std::string& metaserver_addr) {
    auto channel = grpc::CreateChannel(metaserver_addr, grpc::InsecureChannelCredentials());
    stub_ = MetaService::NewStub(channel);
}

bool TestClient::allocateChunk(const std::string& filename, int chunk_index,
                              int64_t chunk_size, std::string& chunk_id,
                              std::vector<std::string>& datanode_addrs) {
    ChunkAllocationRequest request;
    request.set_filename(filename);
    request.set_chunk_index(chunk_index);
    request.set_chunk_size(chunk_size);
    
    ChunkLocation response;
    grpc::ClientContext context;
    grpc::Status status = stub_->AllocateChunkLocation(&context, request, &response);
    
    if (status.ok()) {
        chunk_id = response.chunk_id();
        datanode_addrs.clear();
        for (const auto& addr : response.datanode_addresses()) {
            datanode_addrs.push_back(addr);
        }
        return true;
    }
    
    return false;
}

std::vector<ChunkLocationInfo> TestClient::getFileLocation(const std::string& filename) {
    FileLocationRequest request;
    request.set_filename(filename);
    
    FileLocationResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->GetFileLocation(&context, request, &response);
    
    std::vector<ChunkLocationInfo> locations;
    if (status.ok() && response.found()) {
        for (const auto& chunk : response.chunks()) {
            ChunkLocationInfo info;
            info.chunk_id = chunk.chunk_id();
            for (const auto& addr : chunk.datanode_addresses()) {
                info.datanode_addresses.push_back(addr);
            }
            locations.push_back(info);
        }
    }
    
    return locations;
}

// Assertion utilities
void expectFilesEqual(const std::string& file1, const std::string& file2) {
    std::ifstream f1(file1, std::ios::binary);
    std::ifstream f2(file2, std::ios::binary);
    
    if (!f1.is_open() || !f2.is_open()) {
        throw std::runtime_error("Failed to open files for comparison");
    }
    
    std::string content1((std::istreambuf_iterator<char>(f1)), std::istreambuf_iterator<char>());
    std::string content2((std::istreambuf_iterator<char>(f2)), std::istreambuf_iterator<char>());
    
    if (content1 != content2) {
        throw std::runtime_error("Files are not equal");
    }
}

void expectDataEqual(const std::vector<char>& data1, const std::vector<char>& data2) {
    if (data1.size() != data2.size()) {
        throw std::runtime_error("Data sizes don't match: " + std::to_string(data1.size()) + " vs " + std::to_string(data2.size()));
    }
    
    if (data1 != data2) {
        throw std::runtime_error("Data content doesn't match");
    }
}

void expectChunkExists(const std::string& storage_path, const std::string& chunk_id) {
    std::string subdir = chunk_id.substr(0, std::min(size_t(2), chunk_id.length()));
    if (subdir.length() < 2) {
        subdir = "00";
    }
    
    std::string chunk_path = storage_path + "/" + subdir + "/" + chunk_id + ".chunk";
    
    if (!std::filesystem::exists(chunk_path)) {
        throw std::runtime_error("Chunk file doesn't exist: " + chunk_path);
    }
}

// Timer implementation
Timer::Timer() {
    reset();
}

void Timer::reset() {
    start_time_ = std::chrono::high_resolution_clock::now();
}

double Timer::elapsedSeconds() const {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time_);
    return duration.count() / 1000.0;
}

int64_t Timer::elapsedMilliseconds() const {
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time_).count();
}

// Wait utilities
bool waitForCondition(std::function<bool()> condition, 
                     std::chrono::milliseconds timeout,
                     std::chrono::milliseconds poll_interval) {
    auto start = std::chrono::steady_clock::now();
    
    while (std::chrono::steady_clock::now() - start < timeout) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(poll_interval);
    }
    
    return false;
}

bool waitForServerReady(const std::string& address, std::chrono::milliseconds timeout) {
    return waitForCondition([&address]() {
        try {
            auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
            auto state = channel->GetState(true);
            return state == GRPC_CHANNEL_READY || 
                   channel->WaitForStateChange(state, std::chrono::system_clock::now() + std::chrono::milliseconds(100));
        } catch (...) {
            return false;
        }
    }, timeout);
}

} // namespace test_utils