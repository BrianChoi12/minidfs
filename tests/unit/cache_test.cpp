#include <gtest/gtest.h>
#include "cache.hpp"
#include "unit_test_utils.hpp"
#include <thread>
#include <atomic>

class CacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache_ = std::make_unique<Cache>(3); // Small capacity for testing
    }
    
    ChunkLocationInfo createTestChunkInfo(const std::string& chunk_id, 
                                         const std::vector<std::string>& addresses) {
        ChunkLocationInfo info;
        info.chunk_id = chunk_id;
        info.datanode_addresses = addresses;
        return info;
    }
    
    std::unique_ptr<Cache> cache_;
};

TEST_F(CacheTest, BasicPutAndGet) {
    auto info = createTestChunkInfo("chunk1", {"node1", "node2"});
    
    cache_->put("chunk1", info);
    
    auto retrieved = cache_->get("chunk1");
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->chunk_id, "chunk1");
    EXPECT_EQ(retrieved->datanode_addresses.size(), 2);
    EXPECT_EQ(retrieved->datanode_addresses[0], "node1");
    EXPECT_EQ(retrieved->datanode_addresses[1], "node2");
}

TEST_F(CacheTest, GetNonExistentChunk) {
    auto result = cache_->get("nonexistent");
    EXPECT_FALSE(result.has_value());
}

TEST_F(CacheTest, UpdateExistingChunk) {
    auto info1 = createTestChunkInfo("chunk1", {"node1"});
    auto info2 = createTestChunkInfo("chunk1", {"node2", "node3"});
    
    cache_->put("chunk1", info1);
    cache_->put("chunk1", info2);
    
    auto retrieved = cache_->get("chunk1");
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->datanode_addresses.size(), 2);
    EXPECT_EQ(retrieved->datanode_addresses[0], "node2");
    EXPECT_EQ(retrieved->datanode_addresses[1], "node3");
}

TEST_F(CacheTest, LRUEviction) {
    auto info1 = createTestChunkInfo("chunk1", {"node1"});
    auto info2 = createTestChunkInfo("chunk2", {"node2"});
    auto info3 = createTestChunkInfo("chunk3", {"node3"});
    auto info4 = createTestChunkInfo("chunk4", {"node4"});
    
    // Fill cache to capacity
    cache_->put("chunk1", info1);
    cache_->put("chunk2", info2);
    cache_->put("chunk3", info3);
    
    EXPECT_EQ(cache_->size(), 3);
    
    // Add one more item, should evict least recently used (chunk1)
    cache_->put("chunk4", info4);
    
    EXPECT_EQ(cache_->size(), 3);
    EXPECT_FALSE(cache_->get("chunk1").has_value()); // Should be evicted
    EXPECT_TRUE(cache_->get("chunk2").has_value());
    EXPECT_TRUE(cache_->get("chunk3").has_value());
    EXPECT_TRUE(cache_->get("chunk4").has_value());
}

TEST_F(CacheTest, LRUOrderingWithGet) {
    auto info1 = createTestChunkInfo("chunk1", {"node1"});
    auto info2 = createTestChunkInfo("chunk2", {"node2"});
    auto info3 = createTestChunkInfo("chunk3", {"node3"});
    auto info4 = createTestChunkInfo("chunk4", {"node4"});
    
    // Fill cache
    cache_->put("chunk1", info1);
    cache_->put("chunk2", info2);
    cache_->put("chunk3", info3);
    
    // Access chunk1, making it most recently used
    cache_->get("chunk1");
    
    // Add chunk4, should evict chunk2 (now least recently used)
    cache_->put("chunk4", info4);
    
    EXPECT_TRUE(cache_->get("chunk1").has_value());  // Should still exist
    EXPECT_FALSE(cache_->get("chunk2").has_value()); // Should be evicted
    EXPECT_TRUE(cache_->get("chunk3").has_value());
    EXPECT_TRUE(cache_->get("chunk4").has_value());
}

TEST_F(CacheTest, RemoveChunk) {
    auto info1 = createTestChunkInfo("chunk1", {"node1"});
    auto info2 = createTestChunkInfo("chunk2", {"node2"});
    
    cache_->put("chunk1", info1);
    cache_->put("chunk2", info2);
    
    EXPECT_EQ(cache_->size(), 2);
    EXPECT_TRUE(cache_->get("chunk1").has_value());
    
    cache_->remove("chunk1");
    
    EXPECT_EQ(cache_->size(), 1);
    EXPECT_FALSE(cache_->get("chunk1").has_value());
    EXPECT_TRUE(cache_->get("chunk2").has_value());
}

TEST_F(CacheTest, RemoveNonExistentChunk) {
    cache_->remove("nonexistent");
    EXPECT_EQ(cache_->size(), 0);
}

TEST_F(CacheTest, ClearCache) {
    auto info1 = createTestChunkInfo("chunk1", {"node1"});
    auto info2 = createTestChunkInfo("chunk2", {"node2"});
    
    cache_->put("chunk1", info1);
    cache_->put("chunk2", info2);
    
    EXPECT_EQ(cache_->size(), 2);
    
    cache_->clear();
    
    EXPECT_EQ(cache_->size(), 0);
    EXPECT_FALSE(cache_->get("chunk1").has_value());
    EXPECT_FALSE(cache_->get("chunk2").has_value());
}

TEST_F(CacheTest, ThreadSafety) {
    const int num_threads = 10;
    const int operations_per_thread = 100;
    
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    
    // Launch threads that perform concurrent operations
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < operations_per_thread; ++i) {
                std::string chunk_id = "chunk_" + std::to_string(t) + "_" + std::to_string(i);
                std::string node_addr = "node_" + std::to_string(t);
                
                auto info = createTestChunkInfo(chunk_id, {node_addr});
                
                // Put and get
                cache_->put(chunk_id, info);
                auto retrieved = cache_->get(chunk_id);
                
                if (retrieved.has_value() && retrieved->chunk_id == chunk_id) {
                    successful_operations++;
                }
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Due to LRU eviction, not all operations will succeed, but there should be no crashes
    EXPECT_GT(successful_operations.load(), 0);
    EXPECT_LE(cache_->size(), 3); // Capacity limit
}