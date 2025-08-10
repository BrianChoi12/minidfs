#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>
#include <gtest/gtest.h>

namespace unit_test_utils {

// Simple temporary directory for unit tests
class TempDirectory {
private:
    std::string path_;
    bool cleanup_on_destroy_;
    
public:
    explicit TempDirectory(bool cleanup = true) : cleanup_on_destroy_(cleanup) {
        char temp_template[] = "/tmp/minidfs_unit_test_XXXXXX";
        if (mkdtemp(temp_template) == nullptr) {
            throw std::runtime_error("Failed to create temporary directory");
        }
        path_ = temp_template;
    }
    
    ~TempDirectory() {
        if (cleanup_on_destroy_) {
            cleanup();
        }
    }
    
    const std::string& path() const { return path_; }
    
    std::string file_path(const std::string& filename) const {
        return path_ + "/" + filename;
    }
    
    void cleanup() {
        if (std::filesystem::exists(path_)) {
            std::filesystem::remove_all(path_);
        }
    }
};

// Simple temporary file for unit tests
class TempFile {
private:
    std::string path_;
    bool cleanup_on_destroy_;
    
public:
    explicit TempFile(const std::string& content = "", bool cleanup = true) 
        : cleanup_on_destroy_(cleanup) {
        char temp_template[] = "/tmp/minidfs_unit_test_file_XXXXXX";
        int fd = mkstemp(temp_template);
        if (fd == -1) {
            throw std::runtime_error("Failed to create temporary file");
        }
        path_ = temp_template;
        close(fd);
        
        if (!content.empty()) {
            write(content);
        }
    }
    
    ~TempFile() {
        if (cleanup_on_destroy_) {
            cleanup();
        }
    }
    
    const std::string& path() const { return path_; }
    
    void write(const std::string& content) {
        std::ofstream file(path_, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open temp file for writing");
        }
        file.write(content.data(), content.size());
        file.close();
    }
    
    std::string read() const {
        std::ifstream file(path_, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open temp file for reading");
        }
        
        std::ostringstream ss;
        ss << file.rdbuf();
        return ss.str();
    }
    
    size_t size() const {
        return std::filesystem::file_size(path_);
    }
    
    void cleanup() {
        if (std::filesystem::exists(path_)) {
            std::filesystem::remove(path_);
        }
    }
};

// Data generation utilities
inline std::vector<char> generateRandomData(size_t size) {
    std::vector<char> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>(dis(gen));
    }
    
    return data;
}

inline std::string generateRandomString(size_t length) {
    std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, chars.size() - 1);
    
    std::string result;
    result.reserve(length);
    
    for (size_t i = 0; i < length; ++i) {
        result += chars[dis(gen)];
    }
    
    return result;
}

inline std::vector<char> generateZeroData(size_t size) {
    return std::vector<char>(size, 0);
}

inline std::vector<char> generatePatternData(size_t size, const std::string& pattern) {
    std::vector<char> data;
    data.reserve(size);
    
    size_t pattern_pos = 0;
    for (size_t i = 0; i < size; ++i) {
        data.push_back(pattern[pattern_pos]);
        pattern_pos = (pattern_pos + 1) % pattern.size();
    }
    
    return data;
}

// Assertion utilities
inline void expectDataEqual(const std::vector<char>& data1, const std::vector<char>& data2) {
    EXPECT_EQ(data1.size(), data2.size()) << "Data sizes don't match";
    EXPECT_EQ(data1, data2) << "Data content doesn't match";
}

inline void expectFilesEqual(const std::string& file1, const std::string& file2) {
    std::ifstream f1(file1, std::ios::binary);
    std::ifstream f2(file2, std::ios::binary);
    
    ASSERT_TRUE(f1.is_open()) << "Failed to open file: " << file1;
    ASSERT_TRUE(f2.is_open()) << "Failed to open file: " << file2;
    
    std::string content1((std::istreambuf_iterator<char>(f1)), std::istreambuf_iterator<char>());
    std::string content2((std::istreambuf_iterator<char>(f2)), std::istreambuf_iterator<char>());
    
    EXPECT_EQ(content1, content2) << "Files are not equal: " << file1 << " vs " << file2;
}

inline void expectChunkExists(const std::string& storage_path, const std::string& chunk_id) {
    std::string subdir = chunk_id.substr(0, std::min(size_t(2), chunk_id.length()));
    if (subdir.length() < 2) {
        subdir = "00";
    }
    
    std::string chunk_path = storage_path + "/" + subdir + "/" + chunk_id + ".chunk";
    EXPECT_TRUE(std::filesystem::exists(chunk_path)) << "Chunk file doesn't exist: " << chunk_path;
}

} // namespace unit_test_utils