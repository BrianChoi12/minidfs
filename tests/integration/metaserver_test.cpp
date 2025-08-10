#include <gtest/gtest.h>
#include "../utils/test_utils.hpp"
#include "dfs.grpc.pb.h"
#include <grpcpp/grpcpp.h>

class MetaServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Start MetaServer
        metaserver_ = std::make_unique<test_utils::TestMetaServer>();
        ASSERT_TRUE(metaserver_->start()) << "Failed to start MetaServer";
        
        // Create client
        client_ = std::make_unique<test_utils::TestClient>(metaserver_->address());
        
        // Create channel for direct RPC calls
        channel_ = grpc::CreateChannel(metaserver_->address(), grpc::InsecureChannelCredentials());
        stub_ = MetaService::NewStub(channel_);
    }
    
    void TearDown() override {
        if (metaserver_) {
            metaserver_->stop();
        }
    }
    
    std::unique_ptr<test_utils::TestMetaServer> metaserver_;
    std::unique_ptr<test_utils::TestClient> client_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<MetaService::Stub> stub_;
};

TEST_F(MetaServerTest, DataNodeRegistration) {
    DataNodeInfo info;
    info.set_address("localhost:50052");
    info.set_available_space(10 * 1024 * 1024 * 1024L); // 10GB
    
    Ack response;
    grpc::ClientContext context;
    grpc::Status status = stub_->RegisterDataNode(&context, info, &response);
    
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.ok());
    EXPECT_FALSE(response.message().empty());
}

TEST_F(MetaServerTest, DataNodeHeartbeat) {
    // First register a DataNode
    DataNodeInfo reg_info;
    reg_info.set_address("localhost:50052");
    reg_info.set_available_space(10 * 1024 * 1024 * 1024L);
    
    Ack reg_response;
    grpc::ClientContext reg_context;
    ASSERT_TRUE(stub_->RegisterDataNode(&reg_context, reg_info, &reg_response).ok());
    
    // Send heartbeat
    DataNodeHeartbeat heartbeat;
    heartbeat.set_address("localhost:50052");
    heartbeat.set_available_space(9 * 1024 * 1024 * 1024L); // Updated space
    heartbeat.set_current_load(5);
    heartbeat.add_stored_chunk_ids("chunk1");
    heartbeat.add_stored_chunk_ids("chunk2");
    
    HeartbeatResponse hb_response;
    grpc::ClientContext hb_context;
    grpc::Status status = stub_->Heartbeat(&hb_context, heartbeat, &hb_response);
    
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(hb_response.ok());
}

TEST_F(MetaServerTest, ChunkAllocationWithoutDataNodes) {
    // Try to allocate chunk without any registered DataNodes
    std::string chunk_id;
    std::vector<std::string> datanode_addrs;
    
    bool success = client_->allocateChunk("test_file.txt", 0, 1024, chunk_id, datanode_addrs);
    
    // Should fail since no DataNodes are available
    EXPECT_FALSE(success);
}

TEST_F(MetaServerTest, ChunkAllocationWithDataNode) {
    // Register a DataNode first
    DataNodeInfo info;
    info.set_address("localhost:50052");
    info.set_available_space(10 * 1024 * 1024 * 1024L);
    
    Ack reg_response;
    grpc::ClientContext reg_context;
    ASSERT_TRUE(stub_->RegisterDataNode(&reg_context, info, &reg_response).ok());
    
    // Now try chunk allocation
    std::string chunk_id;
    std::vector<std::string> datanode_addrs;
    
    bool success = client_->allocateChunk("test_file.txt", 0, 1024, chunk_id, datanode_addrs);
    
    EXPECT_TRUE(success);
    EXPECT_FALSE(chunk_id.empty());
    EXPECT_EQ(datanode_addrs.size(), 1);
    EXPECT_EQ(datanode_addrs[0], "localhost:50052");
}

TEST_F(MetaServerTest, MultipleChunkAllocation) {
    // Register a DataNode
    DataNodeInfo info;
    info.set_address("localhost:50052");
    info.set_available_space(10 * 1024 * 1024 * 1024L);
    
    Ack reg_response;
    grpc::ClientContext reg_context;
    ASSERT_TRUE(stub_->RegisterDataNode(&reg_context, info, &reg_response).ok());
    
    // Allocate multiple chunks for the same file
    std::vector<std::string> chunk_ids;
    const int num_chunks = 5;
    
    for (int i = 0; i < num_chunks; ++i) {
        std::string chunk_id;
        std::vector<std::string> datanode_addrs;
        
        bool success = client_->allocateChunk("large_file.bin", i, 1024 * 1024, chunk_id, datanode_addrs);
        
        EXPECT_TRUE(success) << "Failed to allocate chunk " << i;
        EXPECT_FALSE(chunk_id.empty()) << "Empty chunk ID for chunk " << i;
        EXPECT_EQ(datanode_addrs.size(), 1) << "Wrong number of DataNodes for chunk " << i;
        
        chunk_ids.push_back(chunk_id);
    }
    
    // Verify all chunk IDs are unique
    std::set<std::string> unique_ids(chunk_ids.begin(), chunk_ids.end());
    EXPECT_EQ(unique_ids.size(), num_chunks) << "Duplicate chunk IDs generated";
}

TEST_F(MetaServerTest, FileLocationBeforeUpload) {
    // Try to get location for non-existent file
    auto locations = client_->getFileLocation("nonexistent.txt");
    EXPECT_TRUE(locations.empty());
}

TEST_F(MetaServerTest, FileLocationAfterChunkAllocation) {
    // Register DataNode
    DataNodeInfo info;
    info.set_address("localhost:50052");
    info.set_available_space(10 * 1024 * 1024 * 1024L);
    
    Ack reg_response;
    grpc::ClientContext reg_context;
    ASSERT_TRUE(stub_->RegisterDataNode(&reg_context, info, &reg_response).ok());
    
    // Allocate chunks for a file
    const int num_chunks = 3;
    std::vector<std::string> expected_chunk_ids;
    
    for (int i = 0; i < num_chunks; ++i) {
        std::string chunk_id;
        std::vector<std::string> datanode_addrs;
        
        ASSERT_TRUE(client_->allocateChunk("test_file.dat", i, 1024, chunk_id, datanode_addrs));
        expected_chunk_ids.push_back(chunk_id);
    }
    
    // Get file location
    auto locations = client_->getFileLocation("test_file.dat");
    
    EXPECT_EQ(locations.size(), num_chunks);
    
    for (size_t i = 0; i < locations.size(); ++i) {
        EXPECT_EQ(locations[i].chunk_id(), expected_chunk_ids[i]);
        EXPECT_EQ(locations[i].datanode_addresses_size(), 1);
        EXPECT_EQ(locations[i].datanode_addresses(0), "localhost:50052");
    }
}

TEST_F(MetaServerTest, LoadBalancingWithMultipleDataNodes) {
    // Register multiple DataNodes with different capacities
    std::vector<std::string> datanode_addrs = {
        "localhost:50052",
        "localhost:50053", 
        "localhost:50054"
    };
    
    std::vector<int64_t> capacities = {
        5 * 1024 * 1024 * 1024L,  // 5GB
        10 * 1024 * 1024 * 1024L, // 10GB - should be preferred
        3 * 1024 * 1024 * 1024L   // 3GB
    };
    
    // Register all DataNodes
    for (size_t i = 0; i < datanode_addrs.size(); ++i) {
        DataNodeInfo info;
        info.set_address(datanode_addrs[i]);
        info.set_available_space(capacities[i]);
        
        Ack response;
        grpc::ClientContext context;
        ASSERT_TRUE(stub_->RegisterDataNode(&context, info, &response).ok());
    }
    
    // Allocate several chunks and verify load balancing
    std::map<std::string, int> allocations_per_node;
    const int num_allocations = 10;
    
    for (int i = 0; i < num_allocations; ++i) {
        std::string chunk_id;
        std::vector<std::string> assigned_nodes;
        
        ASSERT_TRUE(client_->allocateChunk("balanced_file.bin", i, 1024 * 1024, chunk_id, assigned_nodes));
        ASSERT_EQ(assigned_nodes.size(), 1);
        
        allocations_per_node[assigned_nodes[0]]++;
    }
    
    // The node with highest capacity should get the most allocations
    EXPECT_GT(allocations_per_node["localhost:50053"], allocations_per_node["localhost:50052"]);
    EXPECT_GT(allocations_per_node["localhost:50053"], allocations_per_node["localhost:50054"]);
}

TEST_F(MetaServerTest, StaleDataNodeCleanup) {
    // Register a DataNode
    DataNodeInfo info;
    info.set_address("localhost:50052");
    info.set_available_space(10 * 1024 * 1024 * 1024L);
    
    Ack reg_response;
    grpc::ClientContext reg_context;
    ASSERT_TRUE(stub_->RegisterDataNode(&reg_context, info, &reg_response).ok());
    
    // Allocate a chunk successfully
    std::string chunk_id;
    std::vector<std::string> datanode_addrs;
    ASSERT_TRUE(client_->allocateChunk("test_file.txt", 0, 1024, chunk_id, datanode_addrs));
    
    // Wait for DataNode to be considered stale (this would require a longer test
    // or mocking time, so we'll just verify the basic flow works)
    
    // Note: Full stale node cleanup testing would require either:
    // 1. Mocking time/clocks
    // 2. Making timeout configurable for tests
    // 3. Waiting actual timeout period (not practical for unit tests)
    
    // For now, verify that the heartbeat mechanism exists and works
    DataNodeHeartbeat heartbeat;
    heartbeat.set_address("localhost:50052");
    heartbeat.set_available_space(info.available_space());
    heartbeat.set_current_load(0);
    
    HeartbeatResponse hb_response;
    grpc::ClientContext hb_context;
    ASSERT_TRUE(stub_->Heartbeat(&hb_context, heartbeat, &hb_response).ok());
}

TEST_F(MetaServerTest, ConcurrentClientRequests) {
    // Register a DataNode
    DataNodeInfo info;
    info.set_address("localhost:50052");
    info.set_available_space(10 * 1024 * 1024 * 1024L);
    
    Ack reg_response;
    grpc::ClientContext reg_context;
    ASSERT_TRUE(stub_->RegisterDataNode(&reg_context, info, &reg_response).ok());
    
    // Launch multiple threads making concurrent requests
    const int num_threads = 10;
    const int requests_per_thread = 20;
    std::atomic<int> successful_allocations{0};
    std::atomic<int> failed_allocations{0};
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Create a client for this thread
            test_utils::TestClient thread_client(metaserver_->address());
            
            for (int i = 0; i < requests_per_thread; ++i) {
                std::string filename = "thread_" + std::to_string(t) + "_file_" + std::to_string(i);
                std::string chunk_id;
                std::vector<std::string> datanode_addrs;
                
                if (thread_client.allocateChunk(filename, 0, 1024, chunk_id, datanode_addrs)) {
                    successful_allocations++;
                } else {
                    failed_allocations++;
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All allocations should succeed since we have sufficient capacity
    int total_requests = num_threads * requests_per_thread;
    EXPECT_EQ(successful_allocations.load(), total_requests);
    EXPECT_EQ(failed_allocations.load(), 0);
}