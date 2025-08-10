#include "manager.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <random>

Manager::Manager(Cache* aCache) : theCache(aCache) {
}

std::string Manager::generateChunkId(const std::string& filename, int chunk_index) {
    // Generate unique chunk ID using filename, index, and counter
    std::stringstream ss;
    ss << std::hash<std::string>{}(filename) << "_" 
       << chunk_index << "_" 
       << chunk_counter.fetch_add(1);
    return ss.str();
}

std::string Manager::selectDataNodeForChunk(int64_t chunk_size) {
    std::lock_guard<std::mutex> lock(datanodes_mutex);
    
    // Clean up stale nodes first
    cleanupStaleDataNodes();
    
    std::string best_node;
    int64_t max_space = 0;
    int32_t min_load = INT32_MAX;
    
    for (const auto& [address, state] : datanodes) {
        // Skip nodes without enough space
        if (state.available_space < chunk_size) {
            continue;
        }
        
        // Select node with lowest load and most available space
        if (state.current_load < min_load || 
            (state.current_load == min_load && state.available_space > max_space)) {
            best_node = address;
            max_space = state.available_space;
            min_load = state.current_load;
        }
    }
    
    return best_node;
}

std::vector<std::string> Manager::getActiveDataNodes() {
    std::lock_guard<std::mutex> lock(datanodes_mutex);
    std::vector<std::string> active_nodes;
    
    auto now = std::chrono::steady_clock::now();
    for (const auto& [address, state] : datanodes) {
        // Consider node active if heartbeat received within last 30 seconds
        auto time_since_heartbeat = std::chrono::duration_cast<std::chrono::seconds>(
            now - state.last_heartbeat).count();
        if (time_since_heartbeat < 30) {
            active_nodes.push_back(address);
        }
    }
    
    return active_nodes;
}

void Manager::cleanupStaleDataNodes() {
    // Should be called with datanodes_mutex locked
    auto now = std::chrono::steady_clock::now();
    std::vector<std::string> stale_nodes;
    
    for (const auto& [address, state] : datanodes) {
        auto time_since_heartbeat = std::chrono::duration_cast<std::chrono::seconds>(
            now - state.last_heartbeat).count();
        if (time_since_heartbeat > 60) {  // Remove nodes after 60 seconds of no heartbeat
            stale_nodes.push_back(address);
        }
    }
    
    for (const auto& address : stale_nodes) {
        std::cout << "[INFO] Removing stale DataNode: " << address << "\n";
        datanodes.erase(address);
    }
}

bool Manager::registerDataNode(const std::string& address, int64_t available_space) {
    std::lock_guard<std::mutex> lock(datanodes_mutex);
    
    DataNodeState state;
    state.address = address;
    state.available_space = available_space;
    state.current_load = 0;
    state.last_heartbeat = std::chrono::steady_clock::now();
    
    datanodes[address] = state;
    std::cout << "[INFO] Registered DataNode: " << address 
              << " with " << available_space << " bytes available\n";
    return true;
}

bool Manager::updateDataNodeHeartbeat(const std::string& address,
                                      const std::vector<std::string>& stored_chunks,
                                      int64_t available_space,
                                      int32_t current_load) {
    {
        std::lock_guard<std::mutex> lock(datanodes_mutex);
        
        auto it = datanodes.find(address);
        if (it == datanodes.end()) {
            // Auto-register unknown DataNode
            DataNodeState state;
            state.address = address;
            state.available_space = available_space;
            state.current_load = current_load;
            state.stored_chunks = std::unordered_set<std::string>(
                stored_chunks.begin(), stored_chunks.end());
            state.last_heartbeat = std::chrono::steady_clock::now();
            datanodes[address] = state;
            std::cout << "[INFO] Auto-registered DataNode from heartbeat: " << address << "\n";
        } else {
            // Update existing DataNode
            it->second.available_space = available_space;
            it->second.current_load = current_load;
            it->second.stored_chunks = std::unordered_set<std::string>(
                stored_chunks.begin(), stored_chunks.end());
            it->second.last_heartbeat = std::chrono::steady_clock::now();
        }
    }
    
    // Update chunk to DataNode mapping
    {
        std::lock_guard<std::mutex> lock(chunks_mutex);
        for (const auto& chunk_id : stored_chunks) {
            auto& nodes = chunk_to_datanodes[chunk_id];
            if (std::find(nodes.begin(), nodes.end(), address) == nodes.end()) {
                nodes.push_back(address);
            }
        }
    }
    
    return true;
}

std::pair<std::string, std::vector<std::string>> Manager::allocateChunkLocation(
    const std::string& filename,
    int32_t chunk_index,
    int64_t chunk_size) {
    
    // Generate unique chunk ID
    std::string chunk_id = generateChunkId(filename, chunk_index);
    
    // Select DataNode for this chunk
    std::string selected_node = selectDataNodeForChunk(chunk_size);
    
    if (selected_node.empty()) {
        std::cerr << "[ERROR] No available DataNode for chunk allocation\n";
        return {"", {}};
    }
    
    // Update file metadata
    {
        std::lock_guard<std::mutex> lock(files_mutex);
        auto& file_meta = files[filename];
        
        if (file_meta.chunk_ids.size() <= static_cast<size_t>(chunk_index)) {
            file_meta.chunk_ids.resize(chunk_index + 1);
        }
        file_meta.chunk_ids[chunk_index] = chunk_id;
        file_meta.filename = filename;
        file_meta.total_size += chunk_size;
        
        if (chunk_index == 0) {
            file_meta.created_at = std::chrono::system_clock::now();
        }
    }
    
    // Reserve this chunk for the selected DataNode
    {
        std::lock_guard<std::mutex> lock(chunks_mutex);
        chunk_to_datanodes[chunk_id] = {selected_node};
    }
    
    // Update DataNode's expected load
    {
        std::lock_guard<std::mutex> lock(datanodes_mutex);
        if (datanodes.find(selected_node) != datanodes.end()) {
            datanodes[selected_node].current_load++;
            datanodes[selected_node].available_space -= chunk_size;
        }
    }
    
    std::cout << "[INFO] Allocated chunk " << chunk_id 
              << " for file " << filename 
              << " (index " << chunk_index << ") to DataNode " << selected_node << "\n";
    
    return {chunk_id, {selected_node}};
}

std::vector<ChunkLocationInfo> Manager::getFileLocation(const std::string& filename) {
    std::vector<ChunkLocationInfo> locations;
    
    // Check if file exists
    std::vector<std::string> chunk_ids;
    {
        std::lock_guard<std::mutex> lock(files_mutex);
        auto it = files.find(filename);
        if (it == files.end()) {
            return locations;  // File not found
        }
        chunk_ids = it->second.chunk_ids;
    }
    
    // Get location for each chunk
    for (const auto& chunk_id : chunk_ids) {
        if (chunk_id.empty()) {
            continue;  // Skip empty chunks (sparse file)
        }
        
        // First check cache
        auto cached = theCache->get(chunk_id);
        if (cached.has_value()) {
            locations.push_back(cached.value());
            continue;
        }
        
        // Not in cache, look up from chunk mapping
        ChunkLocationInfo info;
        info.chunk_id = chunk_id;
        
        {
            std::lock_guard<std::mutex> lock(chunks_mutex);
            auto it = chunk_to_datanodes.find(chunk_id);
            if (it != chunk_to_datanodes.end()) {
                // Filter out stale DataNodes
                std::vector<std::string> active_nodes = getActiveDataNodes();
                for (const auto& node : it->second) {
                    if (std::find(active_nodes.begin(), active_nodes.end(), node) != active_nodes.end()) {
                        info.datanode_addresses.push_back(node);
                    }
                }
            }
        }
        
        if (!info.datanode_addresses.empty()) {
            // Add to cache for future requests
            theCache->put(chunk_id, info);
            locations.push_back(info);
        }
    }
    
    return locations;
}

void Manager::removeDataNode(const std::string& address) {
    std::lock_guard<std::mutex> lock(datanodes_mutex);
    datanodes.erase(address);
    std::cout << "[INFO] Removed DataNode: " << address << "\n";
}

size_t Manager::getDataNodeCount() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(datanodes_mutex));
    return datanodes.size();
}

size_t Manager::getFileCount() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(files_mutex));
    return files.size();
}