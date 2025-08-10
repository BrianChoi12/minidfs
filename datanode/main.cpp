#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <csignal>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include "dfs.grpc.pb.h"
#include "storage.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// Global flag for graceful shutdown
std::atomic<bool> running{true};

class DataNodeServiceImpl final : public DataNodeService::Service {
private:
    DataNodeStorage* storage;
    
public:
    explicit DataNodeServiceImpl(DataNodeStorage* storage) : storage(storage) {}
    
    Status StoreChunk(ServerContext* context, const ::ChunkData* request, ::Ack* response) override {
        storage->incrementLoad();
        
        // Convert protobuf bytes to vector
        std::vector<char> data(request->data().begin(), request->data().end());
        
        bool success = storage->storeChunk(request->chunk_id(), data);
        
        response->set_ok(success);
        if (success) {
            response->set_message("Chunk stored successfully");
        } else {
            response->set_message("Failed to store chunk");
        }
        
        storage->decrementLoad();
        return Status::OK;
    }
    
    Status ReadChunk(ServerContext* context, const ::ChunkRequest* request, ::ChunkData* response) override {
        storage->incrementLoad();
        
        std::vector<char> data = storage->readChunk(request->chunk_id());
        
        if (!data.empty()) {
            response->set_chunk_id(request->chunk_id());
            response->set_data(data.data(), data.size());
        } else {
            storage->decrementLoad();
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Chunk not found");
        }
        
        storage->decrementLoad();
        return Status::OK;
    }
};

// Heartbeat thread function
void heartbeatThread(const std::string& metaserver_addr, 
                    const std::string& datanode_addr,
                    DataNodeStorage* storage) {
    
    // Create channel to MetaServer
    auto channel = grpc::CreateChannel(metaserver_addr, grpc::InsecureChannelCredentials());
    auto stub = MetaService::NewStub(channel);
    
    // Register with MetaServer initially
    {
        DataNodeInfo info;
        info.set_address(datanode_addr);
        info.set_available_space(storage->getAvailableSpace());
        
        Ack ack;
        grpc::ClientContext context;
        grpc::Status status = stub->RegisterDataNode(&context, info, &ack);
        
        if (status.ok() && ack.ok()) {
            std::cout << "[INFO] Registered with MetaServer at " << metaserver_addr << "\n";
        } else {
            std::cerr << "[ERROR] Failed to register with MetaServer: " 
                      << status.error_message() << "\n";
        }
    }
    
    // Send periodic heartbeats
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(10));  // Heartbeat every 10 seconds
        
        if (!running.load()) break;
        
        DataNodeHeartbeat heartbeat;
        heartbeat.set_address(datanode_addr);
        heartbeat.set_available_space(storage->getAvailableSpace());
        heartbeat.set_current_load(storage->getCurrentLoad());
        
        // Add all stored chunk IDs
        auto chunk_ids = storage->getStoredChunkIds();
        for (const auto& chunk_id : chunk_ids) {
            heartbeat.add_stored_chunk_ids(chunk_id);
        }
        
        HeartbeatResponse response;
        grpc::ClientContext context;
        
        // Set deadline for heartbeat
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        context.set_deadline(deadline);
        
        grpc::Status status = stub->Heartbeat(&context, heartbeat, &response);
        
        if (status.ok() && response.ok()) {
            std::cout << "[HEARTBEAT] Sent successfully - " 
                      << "Chunks: " << chunk_ids.size() 
                      << ", Available: " << storage->getAvailableSpace() / (1024*1024) << " MB"
                      << ", Load: " << storage->getCurrentLoad() << "\n";
            
            // Handle cleanup requests from MetaServer
            if (response.chunks_to_delete_size() > 0) {
                for (const auto& chunk_id : response.chunks_to_delete()) {
                    storage->deleteChunk(chunk_id);
                    std::cout << "[INFO] Deleted chunk as requested by MetaServer: " 
                              << chunk_id << "\n";
                }
            }
        } else {
            std::cerr << "[WARNING] Heartbeat failed: " << status.error_message() << "\n";
        }
    }
    
    std::cout << "[INFO] Heartbeat thread exiting\n";
}

void runDataNode(const std::string& datanode_addr, 
                const std::string& metaserver_addr,
                const std::string& storage_path,
                int64_t storage_capacity) {
    
    // Initialize storage
    DataNodeStorage storage(storage_path, storage_capacity);
    
    // Perform initial health check
    if (!storage.performHealthCheck()) {
        std::cerr << "[WARNING] Health check found issues, continuing anyway\n";
    }
    
    // Create and start the RPC service
    DataNodeServiceImpl service(&storage);
    
    ServerBuilder builder;
    builder.AddListeningPort(datanode_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    
    std::unique_ptr<Server> server(builder.BuildAndStart());
    
    if (!server) {
        std::cerr << "[ERROR] Failed to start DataNode server\n";
        return;
    }
    
    std::cout << "[INFO] DataNode server listening on " << datanode_addr << "\n";
    std::cout << "[INFO] Storage path: " << storage_path << "\n";
    std::cout << "[INFO] Storage capacity: " << storage_capacity / (1024*1024*1024) << " GB\n";
    std::cout << "[INFO] MetaServer address: " << metaserver_addr << "\n";
    
    // Start heartbeat thread
    std::thread heartbeat(heartbeatThread, metaserver_addr, datanode_addr, &storage);
    
    // Handle shutdown signal
    signal(SIGINT, [](int) { 
        std::cout << "\n[INFO] Shutdown signal received\n";
        running = false; 
    });
    
    // Wait for server to shutdown
    server->Wait();
    
    // Clean shutdown
    running = false;
    if (heartbeat.joinable()) {
        heartbeat.join();
    }
    
    std::cout << "[INFO] DataNode shutdown complete\n";
}

int main(int argc, char* argv[]) {
    // Parse command line arguments
    std::string datanode_addr = "0.0.0.0:50052";  // Default DataNode address
    std::string metaserver_addr = "localhost:50051";  // Default MetaServer address
    std::string storage_path = "./datanode_storage";  // Default storage path
    int64_t storage_capacity = 10L * 1024 * 1024 * 1024;  // Default 10GB
    
    // Simple argument parsing
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--datanode-addr" && i + 1 < argc) {
            datanode_addr = argv[++i];
        } else if (arg == "--metaserver-addr" && i + 1 < argc) {
            metaserver_addr = argv[++i];
        } else if (arg == "--storage-path" && i + 1 < argc) {
            storage_path = argv[++i];
        } else if (arg == "--storage-capacity" && i + 1 < argc) {
            storage_capacity = std::stoll(argv[++i]) * 1024 * 1024 * 1024;  // Convert GB to bytes
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --datanode-addr <addr>     DataNode listen address (default: 0.0.0.0:50052)\n"
                      << "  --metaserver-addr <addr>   MetaServer address (default: localhost:50051)\n"
                      << "  --storage-path <path>      Storage directory path (default: ./datanode_storage)\n"
                      << "  --storage-capacity <GB>    Storage capacity in GB (default: 10)\n"
                      << "  --help                     Show this help message\n";
            return 0;
        }
    }
    
    std::cout << "====================================\n";
    std::cout << "       MiniDFS DataNode Starting    \n";
    std::cout << "====================================\n";
    
    runDataNode(datanode_addr, metaserver_addr, storage_path, storage_capacity);
    
    return 0;
}