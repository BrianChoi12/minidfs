#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <filesystem>
#include <atomic>
#include <chrono>

struct ChunkMetadata {
    std::string chunk_id;
    size_t size;
    std::string checksum;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_accessed;
};

class DataNodeStorage {
private:
    std::string storage_path;
    std::atomic<int64_t> total_capacity;
    std::atomic<int64_t> used_space;
    std::atomic<int32_t> current_load;
    
    // Thread-safe chunk metadata tracking
    mutable std::mutex metadata_mutex;
    std::unordered_map<std::string, ChunkMetadata> chunk_metadata;
    
    // Helper methods
    std::string getChunkPath(const std::string& chunk_id) const;
    std::string calculateChecksum(const std::vector<char>& data) const;
    bool verifyChecksum(const std::string& chunk_id, const std::vector<char>& data) const;
    void ensureStorageDirectory();
    void loadExistingChunks();
    
public:
    explicit DataNodeStorage(const std::string& storage_path, int64_t capacity_bytes = 10L * 1024 * 1024 * 1024); // Default 10GB
    ~DataNodeStorage();
    
    // Chunk operations
    bool storeChunk(const std::string& chunk_id, const std::vector<char>& data);
    std::vector<char> readChunk(const std::string& chunk_id);
    bool deleteChunk(const std::string& chunk_id);
    bool hasChunk(const std::string& chunk_id) const;
    
    // Status and metrics
    std::vector<std::string> getStoredChunkIds() const;
    int64_t getAvailableSpace() const;
    int64_t getUsedSpace() const;
    int32_t getCurrentLoad() const;
    void incrementLoad();
    void decrementLoad();
    
    // Maintenance
    bool performHealthCheck();
    void cleanupOrphanedChunks(const std::vector<std::string>& valid_chunks);
};