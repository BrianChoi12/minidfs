#include "cache.hpp"

Cache::Cache(size_t capacity) : capacity(capacity) {
    if (capacity == 0) {
        this->capacity = 1; // Minimum capacity of 1
    }
}

void Cache::touch(CacheList::iterator it) {
    // Move the accessed item to the front of the list
    auto pair = *it;
    cache_list.erase(it);
    cache_list.push_front(pair);
    cache_map[pair.first] = cache_list.begin();
}

void Cache::evict() {
    // Remove the least recently used item (from the back)
    if (!cache_list.empty()) {
        auto last = cache_list.back();
        cache_map.erase(last.first);
        cache_list.pop_back();
    }
}

void Cache::put(const std::string& chunk_id, const ChunkLocationInfo& location) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    
    // Check if the chunk already exists in cache
    auto it = cache_map.find(chunk_id);
    if (it != cache_map.end()) {
        // Update existing entry and move to front
        it->second->second = location;
        touch(it->second);
    } else {
        // Add new entry
        if (cache_list.size() >= capacity) {
            evict();
        }
        cache_list.push_front({chunk_id, location});
        cache_map[chunk_id] = cache_list.begin();
    }
}

std::optional<ChunkLocationInfo> Cache::get(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    
    auto it = cache_map.find(chunk_id);
    if (it != cache_map.end()) {
        // Found - move to front and return
        touch(it->second);
        return it->second->second;
    }
    return std::nullopt;
}

void Cache::remove(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    
    auto it = cache_map.find(chunk_id);
    if (it != cache_map.end()) {
        cache_list.erase(it->second);
        cache_map.erase(it);
    }
}

void Cache::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex);
    cache_list.clear();
    cache_map.clear();
}

size_t Cache::size() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(cache_mutex));
    return cache_list.size();
}