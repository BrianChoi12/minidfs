#pragma once 

#include <unordered_map>
#include <list>
#include <string>
#include <vector>
#include <mutex>
#include <optional>

struct ChunkLocationInfo {
    std::string chunk_id;
    std::vector<std::string> datanode_addresses;
};

class Cache {
private:
    size_t capacity;
    std::mutex cache_mutex;
    
    // LRU cache implementation using list + unordered_map
    // List maintains the order (front = most recently used, back = least recently used)
    using CacheList = std::list<std::pair<std::string, ChunkLocationInfo>>;
    CacheList cache_list;
    
    // Map for O(1) lookup: chunk_id -> iterator to list node
    std::unordered_map<std::string, CacheList::iterator> cache_map;
    
    // Move an element to the front (mark as most recently used)
    void touch(CacheList::iterator it);
    
    // Evict the least recently used item
    void evict();

public:
    explicit Cache(size_t capacity = 1000);
    
    // Insert or update a chunk location in the cache
    void put(const std::string& chunk_id, const ChunkLocationInfo& location);
    
    // Get chunk location from cache (returns nullopt if not found)
    std::optional<ChunkLocationInfo> get(const std::string& chunk_id);
    
    // Remove a chunk from cache (e.g., when chunk is deleted)
    void remove(const std::string& chunk_id);
    
    // Clear all entries
    void clear();
    
    // Get current size
    size_t size() const;
};
