#pragma once

#include "cache.hpp"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <mutex>
#include <chrono>
#include <atomic>

struct DataNodeState {
    std::string address;
    int64_t available_space;
    int32_t current_load;
    std::unordered_set<std::string> stored_chunks;
    std::chrono::steady_clock::time_point last_heartbeat;
};

struct FileMetadata {
    std::string filename;
    std::vector<std::string> chunk_ids;  // Ordered list of chunks for this file
    int64_t total_size;
    std::chrono::system_clock::time_point created_at;
};

class Manager {
private:
    Cache* theCache;
    
    // DataNode management
    std::mutex datanodes_mutex;
    std::unordered_map<std::string, DataNodeState> datanodes;  // address -> state
    
    // File metadata management
    std::mutex files_mutex;
    std::unordered_map<std::string, FileMetadata> files;  // filename -> metadata
    
    // Chunk to DataNode mapping
    std::mutex chunks_mutex;
    std::unordered_map<std::string, std::vector<std::string>> chunk_to_datanodes;  // chunk_id -> datanode addresses
    
    // Chunk ID generation
    std::atomic<uint64_t> chunk_counter{0};
    
    // Helper methods
    std::string generateChunkId(const std::string& filename, int chunk_index);
    std::string selectDataNodeForChunk(int64_t chunk_size);
    std::vector<std::string> getActiveDataNodes();
    void cleanupStaleDataNodes();
    
public: 
    Manager(Cache* aCache);
    
    // DataNode management
    bool registerDataNode(const std::string& address, int64_t available_space);
    bool updateDataNodeHeartbeat(const std::string& address, 
                                  const std::vector<std::string>& stored_chunks,
                                  int64_t available_space,
                                  int32_t current_load);
    
    // File operations
    std::pair<std::string, std::vector<std::string>> allocateChunkLocation(
        const std::string& filename, 
        int32_t chunk_index, 
        int64_t chunk_size);
    
    std::pair<bool, std::vector<ChunkLocationInfo>> getFileLocation(const std::string& filename);
    
    // Utility
    void removeDataNode(const std::string& address);
    size_t getDataNodeCount() const;
    size_t getFileCount() const;
};
