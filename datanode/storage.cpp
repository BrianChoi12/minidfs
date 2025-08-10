#include "storage.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <unordered_set>
#include <openssl/sha.h>

namespace fs = std::filesystem;

DataNodeStorage::DataNodeStorage(const std::string& storage_path, int64_t capacity_bytes) 
    : storage_path(storage_path), total_capacity(capacity_bytes), used_space(0), current_load(0) {
    
    ensureStorageDirectory();
    loadExistingChunks();
    
    std::cout << "[INFO] DataNode storage initialized at " << storage_path 
              << " with capacity " << capacity_bytes / (1024*1024) << " MB\n";
    std::cout << "[INFO] Found " << chunk_metadata.size() << " existing chunks, "
              << "using " << used_space.load() / (1024*1024) << " MB\n";
}

DataNodeStorage::~DataNodeStorage() {
    // Cleanup if needed
}

void DataNodeStorage::ensureStorageDirectory() {
    fs::create_directories(storage_path);
    
    // Create subdirectories for better file organization
    // Using two-level hierarchy based on first 4 chars of chunk_id
    for (int i = 0; i < 256; ++i) {
        std::stringstream ss;
        ss << std::hex << std::setfill('0') << std::setw(2) << i;
        fs::create_directories(fs::path(storage_path) / ss.str());
    }
}

void DataNodeStorage::loadExistingChunks() {
    std::lock_guard<std::mutex> lock(metadata_mutex);
    
    for (const auto& dir_entry : fs::recursive_directory_iterator(storage_path)) {
        if (dir_entry.is_regular_file() && dir_entry.path().extension() == ".chunk") {
            std::string chunk_id = dir_entry.path().stem().string();
            
            ChunkMetadata metadata;
            metadata.chunk_id = chunk_id;
            metadata.size = dir_entry.file_size();
            metadata.created_at = std::chrono::system_clock::now(); // Approximate
            metadata.last_accessed = metadata.created_at;
            
            // Load checksum from .meta file if exists
            fs::path meta_path = dir_entry.path();
            meta_path.replace_extension(".meta");
            if (fs::exists(meta_path)) {
                std::ifstream meta_file(meta_path);
                if (meta_file.is_open()) {
                    std::getline(meta_file, metadata.checksum);
                }
            }
            
            chunk_metadata[chunk_id] = metadata;
            used_space += metadata.size;
        }
    }
}

std::string DataNodeStorage::getChunkPath(const std::string& chunk_id) const {
    // Use first 2 hex chars for directory (distributes across 256 dirs)
    std::string subdir = chunk_id.substr(0, std::min(size_t(2), chunk_id.length()));
    if (subdir.length() < 2) {
        subdir = "00";  // Default for short IDs
    }
    
    fs::path chunk_path = fs::path(storage_path) / subdir / (chunk_id + ".chunk");
    return chunk_path.string();
}

std::string DataNodeStorage::calculateChecksum(const std::vector<char>& data) const {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return ss.str();
}

bool DataNodeStorage::verifyChecksum(const std::string& chunk_id, const std::vector<char>& data) const {
    std::lock_guard<std::mutex> lock(metadata_mutex);
    
    auto it = chunk_metadata.find(chunk_id);
    if (it == chunk_metadata.end() || it->second.checksum.empty()) {
        return true;  // No checksum to verify
    }
    
    std::string calculated = calculateChecksum(data);
    return calculated == it->second.checksum;
}

bool DataNodeStorage::storeChunk(const std::string& chunk_id, const std::vector<char>& data) {
    // Check capacity
    if (used_space.load() + static_cast<int64_t>(data.size()) > total_capacity.load()) {
        std::cerr << "[ERROR] Insufficient storage space for chunk " << chunk_id << "\n";
        return false;
    }
    
    std::string chunk_path = getChunkPath(chunk_id);
    fs::path parent_dir = fs::path(chunk_path).parent_path();
    
    // Ensure parent directory exists
    fs::create_directories(parent_dir);
    
    // Write chunk data
    std::ofstream file(chunk_path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "[ERROR] Failed to open file for writing: " << chunk_path << "\n";
        return false;
    }
    
    file.write(data.data(), data.size());
    file.flush();
    
    if (!file.good()) {
        std::cerr << "[ERROR] Failed to write chunk " << chunk_id << "\n";
        file.close();
        fs::remove(chunk_path);
        return false;
    }
    
    file.close();
    
    // Calculate and store checksum
    std::string checksum = calculateChecksum(data);
    
    // Write metadata file
    std::string meta_path = chunk_path;
    meta_path.replace(meta_path.find(".chunk"), 6, ".meta");
    std::ofstream meta_file(meta_path);
    if (meta_file.is_open()) {
        meta_file << checksum << "\n";
        meta_file << data.size() << "\n";
        meta_file.close();
    }
    
    // Update metadata
    {
        std::lock_guard<std::mutex> lock(metadata_mutex);
        ChunkMetadata metadata;
        metadata.chunk_id = chunk_id;
        metadata.size = data.size();
        metadata.checksum = checksum;
        metadata.created_at = std::chrono::system_clock::now();
        metadata.last_accessed = metadata.created_at;
        
        // Check if chunk already exists (update case)
        auto it = chunk_metadata.find(chunk_id);
        if (it != chunk_metadata.end()) {
            used_space -= it->second.size;
        }
        
        chunk_metadata[chunk_id] = metadata;
        used_space += metadata.size;
    }
    
    std::cout << "[INFO] Stored chunk " << chunk_id 
              << " (" << data.size() << " bytes, checksum: " << checksum.substr(0, 8) << "...)\n";
    
    return true;
}

std::vector<char> DataNodeStorage::readChunk(const std::string& chunk_id) {
    std::string chunk_path = getChunkPath(chunk_id);
    
    if (!fs::exists(chunk_path)) {
        std::cerr << "[ERROR] Chunk not found: " << chunk_id << "\n";
        return {};
    }
    
    std::ifstream file(chunk_path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "[ERROR] Failed to open chunk file: " << chunk_path << "\n";
        return {};
    }
    
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<char> data(size);
    file.read(data.data(), size);
    file.close();
    
    // Verify checksum
    if (!verifyChecksum(chunk_id, data)) {
        std::cerr << "[ERROR] Checksum verification failed for chunk " << chunk_id << "\n";
        return {};
    }
    
    // Update last accessed time
    {
        std::lock_guard<std::mutex> lock(metadata_mutex);
        auto it = chunk_metadata.find(chunk_id);
        if (it != chunk_metadata.end()) {
            it->second.last_accessed = std::chrono::system_clock::now();
        }
    }
    
    std::cout << "[INFO] Read chunk " << chunk_id << " (" << size << " bytes)\n";
    
    return data;
}

bool DataNodeStorage::deleteChunk(const std::string& chunk_id) {
    std::string chunk_path = getChunkPath(chunk_id);
    std::string meta_path = chunk_path;
    meta_path.replace(meta_path.find(".chunk"), 6, ".meta");
    
    bool chunk_removed = fs::remove(chunk_path);
    fs::remove(meta_path);  // Remove metadata file too
    
    if (chunk_removed) {
        std::lock_guard<std::mutex> lock(metadata_mutex);
        auto it = chunk_metadata.find(chunk_id);
        if (it != chunk_metadata.end()) {
            used_space -= it->second.size;
            chunk_metadata.erase(it);
        }
        
        std::cout << "[INFO] Deleted chunk " << chunk_id << "\n";
        return true;
    }
    
    return false;
}

bool DataNodeStorage::hasChunk(const std::string& chunk_id) const {
    std::lock_guard<std::mutex> lock(metadata_mutex);
    return chunk_metadata.find(chunk_id) != chunk_metadata.end();
}

std::vector<std::string> DataNodeStorage::getStoredChunkIds() const {
    std::lock_guard<std::mutex> lock(metadata_mutex);
    std::vector<std::string> chunk_ids;
    chunk_ids.reserve(chunk_metadata.size());
    
    for (const auto& [chunk_id, _] : chunk_metadata) {
        chunk_ids.push_back(chunk_id);
    }
    
    return chunk_ids;
}

int64_t DataNodeStorage::getAvailableSpace() const {
    return total_capacity.load() - used_space.load();
}

int64_t DataNodeStorage::getUsedSpace() const {
    return used_space.load();
}

int32_t DataNodeStorage::getCurrentLoad() const {
    return current_load.load();
}

void DataNodeStorage::incrementLoad() {
    current_load++;
}

void DataNodeStorage::decrementLoad() {
    if (current_load > 0) {
        current_load--;
    }
}

bool DataNodeStorage::performHealthCheck() {
    std::lock_guard<std::mutex> lock(metadata_mutex);
    
    int corrupted_chunks = 0;
    for (const auto& [chunk_id, metadata] : chunk_metadata) {
        std::string chunk_path = getChunkPath(chunk_id);
        if (!fs::exists(chunk_path)) {
            std::cerr << "[WARNING] Missing chunk file: " << chunk_id << "\n";
            corrupted_chunks++;
        }
    }
    
    if (corrupted_chunks > 0) {
        std::cerr << "[WARNING] Health check found " << corrupted_chunks << " issues\n";
        return false;
    }
    
    return true;
}

void DataNodeStorage::cleanupOrphanedChunks(const std::vector<std::string>& valid_chunks) {
    std::lock_guard<std::mutex> lock(metadata_mutex);
    
    std::unordered_set<std::string> valid_set(valid_chunks.begin(), valid_chunks.end());
    std::vector<std::string> to_delete;
    
    for (const auto& [chunk_id, _] : chunk_metadata) {
        if (valid_set.find(chunk_id) == valid_set.end()) {
            to_delete.push_back(chunk_id);
        }
    }
    
    for (const auto& chunk_id : to_delete) {
        deleteChunk(chunk_id);
        std::cout << "[INFO] Cleaned up orphaned chunk: " << chunk_id << "\n";
    }
}