#include <gtest/gtest.h>
#include "storage.hpp"
#include "../utils/test_utils.hpp"
#include <filesystem>

class StorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_dir_ = std::make_unique<test_utils::TempDirectory>();
        storage_ = std::make_unique<DataNodeStorage>(temp_dir_->path(), 10 * 1024 * 1024); // 10MB capacity
    }
    
    void TearDown() override {
        storage_.reset(); // Destroy storage before cleaning up directory
    }
    
    std::unique_ptr<test_utils::TempDirectory> temp_dir_;
    std::unique_ptr<DataNodeStorage> storage_;
};

TEST_F(StorageTest, StoreAndReadSmallChunk) {
    std::string chunk_id = "test_chunk_1";
    std::string content = "Hello, MiniDFS!";
    std::vector<char> data(content.begin(), content.end());
    
    // Store chunk
    EXPECT_TRUE(storage_->storeChunk(chunk_id, data));
    
    // Verify it exists
    EXPECT_TRUE(storage_->hasChunk(chunk_id));
    
    // Read chunk back
    auto read_data = storage_->readChunk(chunk_id);
    EXPECT_EQ(read_data.size(), data.size());
    EXPECT_EQ(read_data, data);
}

TEST_F(StorageTest, StoreLargeChunk) {
    std::string chunk_id = "large_chunk";
    auto data = test_utils::generateRandomData(1024 * 1024); // 1MB
    
    EXPECT_TRUE(storage_->storeChunk(chunk_id, data));
    EXPECT_TRUE(storage_->hasChunk(chunk_id));
    
    auto read_data = storage_->readChunk(chunk_id);
    test_utils::expectDataEqual(data, read_data);
}

TEST_F(StorageTest, StoreZeroSizeChunk) {
    std::string chunk_id = "empty_chunk";
    std::vector<char> empty_data;
    
    EXPECT_TRUE(storage_->storeChunk(chunk_id, empty_data));
    EXPECT_TRUE(storage_->hasChunk(chunk_id));
    
    auto read_data = storage_->readChunk(chunk_id);
    EXPECT_TRUE(read_data.empty());
}

TEST_F(StorageTest, OverwriteExistingChunk) {
    std::string chunk_id = "overwrite_test";
    std::vector<char> data1{'A', 'B', 'C'};
    std::vector<char> data2{'X', 'Y', 'Z', '1', '2', '3'};
    
    // Store initial data
    EXPECT_TRUE(storage_->storeChunk(chunk_id, data1));
    auto initial_used = storage_->getUsedSpace();
    
    // Overwrite with different data
    EXPECT_TRUE(storage_->storeChunk(chunk_id, data2));
    
    // Verify new data
    auto read_data = storage_->readChunk(chunk_id);
    test_utils::expectDataEqual(data2, read_data);
    
    // Verify used space is updated correctly
    auto final_used = storage_->getUsedSpace();
    EXPECT_EQ(final_used - initial_used, static_cast<int64_t>(data2.size() - data1.size()));
}

TEST_F(StorageTest, ReadNonExistentChunk) {
    auto read_data = storage_->readChunk("nonexistent");
    EXPECT_TRUE(read_data.empty());
}

TEST_F(StorageTest, DeleteChunk) {
    std::string chunk_id = "delete_test";
    auto data = test_utils::generateRandomData(1024);
    
    // Store and verify
    EXPECT_TRUE(storage_->storeChunk(chunk_id, data));
    EXPECT_TRUE(storage_->hasChunk(chunk_id));
    
    auto used_before = storage_->getUsedSpace();
    
    // Delete and verify
    EXPECT_TRUE(storage_->deleteChunk(chunk_id));
    EXPECT_FALSE(storage_->hasChunk(chunk_id));
    
    auto used_after = storage_->getUsedSpace();
    EXPECT_EQ(used_before - used_after, static_cast<int64_t>(data.size()));
    
    // Verify file is actually gone
    auto read_data = storage_->readChunk(chunk_id);
    EXPECT_TRUE(read_data.empty());
}

TEST_F(StorageTest, DeleteNonExistentChunk) {
    EXPECT_FALSE(storage_->deleteChunk("nonexistent"));
}

TEST_F(StorageTest, CapacityManagement) {
    // Create storage with very small capacity
    auto small_storage = std::make_unique<DataNodeStorage>(temp_dir_->path() + "/small", 100);
    
    // Try to store chunk larger than capacity
    auto large_data = test_utils::generateRandomData(200);
    EXPECT_FALSE(small_storage->storeChunk("too_large", large_data));
    
    // Store chunk that fits
    auto small_data = test_utils::generateRandomData(50);
    EXPECT_TRUE(small_storage->storeChunk("fits", small_data));
    
    // Try to store another chunk that would exceed capacity
    auto another_data = test_utils::generateRandomData(60);
    EXPECT_FALSE(small_storage->storeChunk("too_much", another_data));
}

TEST_F(StorageTest, SpaceTracking) {
    auto initial_used = storage_->getUsedSpace();
    auto initial_available = storage_->getAvailableSpace();
    
    EXPECT_EQ(initial_used, 0);
    EXPECT_EQ(initial_available, 10 * 1024 * 1024); // 10MB
    
    // Store some chunks
    auto data1 = test_utils::generateRandomData(1024);
    auto data2 = test_utils::generateRandomData(2048);
    
    EXPECT_TRUE(storage_->storeChunk("chunk1", data1));
    EXPECT_TRUE(storage_->storeChunk("chunk2", data2));
    
    auto used_after = storage_->getUsedSpace();
    auto available_after = storage_->getAvailableSpace();
    
    EXPECT_EQ(used_after, static_cast<int64_t>(data1.size() + data2.size()));
    EXPECT_EQ(available_after, initial_available - used_after);
}

TEST_F(StorageTest, LoadTracking) {
    EXPECT_EQ(storage_->getCurrentLoad(), 0);
    
    storage_->incrementLoad();
    EXPECT_EQ(storage_->getCurrentLoad(), 1);
    
    storage_->incrementLoad();
    storage_->incrementLoad();
    EXPECT_EQ(storage_->getCurrentLoad(), 3);
    
    storage_->decrementLoad();
    EXPECT_EQ(storage_->getCurrentLoad(), 2);
    
    // Test that load doesn't go below 0
    storage_->decrementLoad();
    storage_->decrementLoad();
    storage_->decrementLoad();
    EXPECT_EQ(storage_->getCurrentLoad(), 0);
}

TEST_F(StorageTest, GetStoredChunkIds) {
    auto chunk_ids = storage_->getStoredChunkIds();
    EXPECT_TRUE(chunk_ids.empty());
    
    // Store some chunks
    std::vector<std::string> expected_ids = {"chunk1", "chunk2", "chunk3"};
    auto data = test_utils::generateRandomData(100);
    
    for (const auto& id : expected_ids) {
        EXPECT_TRUE(storage_->storeChunk(id, data));
    }
    
    chunk_ids = storage_->getStoredChunkIds();
    EXPECT_EQ(chunk_ids.size(), expected_ids.size());
    
    // Sort both vectors for comparison
    std::sort(chunk_ids.begin(), chunk_ids.end());
    std::sort(expected_ids.begin(), expected_ids.end());
    
    EXPECT_EQ(chunk_ids, expected_ids);
}

TEST_F(StorageTest, HealthCheck) {
    // Initially should pass health check
    EXPECT_TRUE(storage_->performHealthCheck());
    
    // Store a chunk
    auto data = test_utils::generateRandomData(100);
    EXPECT_TRUE(storage_->storeChunk("health_test", data));
    EXPECT_TRUE(storage_->performHealthCheck());
    
    // Manually delete the file to simulate corruption
    auto chunk_path = temp_dir_->path() + "/he/health_test.chunk";
    if (std::filesystem::exists(chunk_path)) {
        std::filesystem::remove(chunk_path);
    }
    
    // Health check should now fail
    EXPECT_FALSE(storage_->performHealthCheck());
}

TEST_F(StorageTest, PersistenceAcrossInstances) {
    std::string chunk_id = "persistent_chunk";
    auto data = test_utils::generateRandomData(1024);
    
    // Store chunk in first instance
    EXPECT_TRUE(storage_->storeChunk(chunk_id, data));
    
    // Destroy first instance
    storage_.reset();
    
    // Create new instance with same storage path
    storage_ = std::make_unique<DataNodeStorage>(temp_dir_->path(), 10 * 1024 * 1024);
    
    // Verify chunk is still there
    EXPECT_TRUE(storage_->hasChunk(chunk_id));
    auto read_data = storage_->readChunk(chunk_id);
    test_utils::expectDataEqual(data, read_data);
    
    // Verify space tracking is restored
    EXPECT_EQ(storage_->getUsedSpace(), static_cast<int64_t>(data.size()));
}

TEST_F(StorageTest, DirectoryStructure) {
    // Test that chunks are stored in correct subdirectories
    std::vector<std::string> chunk_ids = {
        "00abcd", // Should go in 00/ subdirectory
        "11xyz",  // Should go in 11/ subdirectory
        "ff123"   // Should go in ff/ subdirectory
    };
    
    auto data = test_utils::generateRandomData(100);
    
    for (const auto& chunk_id : chunk_ids) {
        EXPECT_TRUE(storage_->storeChunk(chunk_id, data));
        
        // Verify file exists in correct subdirectory
        std::string expected_subdir = chunk_id.substr(0, 2);
        std::string expected_path = temp_dir_->path() + "/" + expected_subdir + "/" + chunk_id + ".chunk";
        EXPECT_TRUE(std::filesystem::exists(expected_path)) 
            << "Chunk file not found at expected path: " << expected_path;
        
        // Verify metadata file exists too
        std::string meta_path = temp_dir_->path() + "/" + expected_subdir + "/" + chunk_id + ".meta";
        EXPECT_TRUE(std::filesystem::exists(meta_path))
            << "Metadata file not found at expected path: " << meta_path;
    }
}

TEST_F(StorageTest, ConcurrentOperations) {
    const int num_threads = 5;
    const int chunks_per_thread = 10;
    std::atomic<int> successful_stores{0};
    std::atomic<int> successful_reads{0};
    
    std::vector<std::thread> threads;
    
    // Launch threads that store chunks concurrently
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < chunks_per_thread; ++i) {
                std::string chunk_id = "thread_" + std::to_string(t) + "_chunk_" + std::to_string(i);
                auto data = test_utils::generateRandomData(100 + i); // Variable size
                
                if (storage_->storeChunk(chunk_id, data)) {
                    successful_stores++;
                    
                    // Try to read it back
                    auto read_data = storage_->readChunk(chunk_id);
                    if (read_data == data) {
                        successful_reads++;
                    }
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All operations should succeed since we have enough capacity
    int expected_operations = num_threads * chunks_per_thread;
    EXPECT_EQ(successful_stores.load(), expected_operations);
    EXPECT_EQ(successful_reads.load(), expected_operations);
    EXPECT_EQ(storage_->getStoredChunkIds().size(), expected_operations);
}