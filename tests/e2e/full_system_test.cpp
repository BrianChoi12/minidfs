#include <gtest/gtest.h>
#include "../utils/test_utils.hpp"
#include "mini_dfs_client.hpp"
#include <filesystem>
#include <thread>

class FullSystemTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directories
        metaserver_temp_ = std::make_unique<test_utils::TempDirectory>();
        datanode_temp_ = std::make_unique<test_utils::TempDirectory>();
        
        // Start MetaServer
        metaserver_ = std::make_unique<test_utils::TestMetaServer>();
        ASSERT_TRUE(metaserver_->start()) << "Failed to start MetaServer";
        
        // Start DataNode
        datanode_ = std::make_unique<test_utils::TestDataNode>(
            test_utils::createTestAddress(),
            metaserver_->address(),
            datanode_temp_->path()
        );
        ASSERT_TRUE(datanode_->start()) << "Failed to start DataNode";
        
        // Give DataNode time to register and send heartbeat
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        // Create client
        auto channel = grpc::CreateChannel(metaserver_->address(), grpc::InsecureChannelCredentials());
        client_ = std::make_unique<MiniDfsClient>(channel);
    }
    
    void TearDown() override {
        if (datanode_) {
            datanode_->stop();
        }
        if (metaserver_) {
            metaserver_->stop();
        }
    }
    
    std::unique_ptr<test_utils::TempDirectory> metaserver_temp_;
    std::unique_ptr<test_utils::TempDirectory> datanode_temp_;
    std::unique_ptr<test_utils::TestMetaServer> metaserver_;
    std::unique_ptr<test_utils::TestDataNode> datanode_;
    std::unique_ptr<MiniDfsClient> client_;
};

TEST_F(FullSystemTest, SmallFileUploadDownload) {
    // Create test file
    std::string content = "Hello MiniDFS! This is a test file for end-to-end testing.";
    test_utils::TempFile test_file(content);
    
    // Upload file
    client_->UploadFile(test_file.path());
    
    // Verify chunk was stored on DataNode
    auto stored_chunks = std::filesystem::directory_iterator(datanode_temp_->path());
    bool found_chunk = false;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(datanode_temp_->path())) {
        if (entry.path().extension() == ".chunk") {
            found_chunk = true;
            break;
        }
    }
    EXPECT_TRUE(found_chunk) << "No chunk files found in DataNode storage";
    
    // Download file to different location
    std::string download_path = test_file.path() + ".downloaded";
    client_->DownloadFile(std::filesystem::path(test_file.path()).filename());
    
    // Verify downloaded file matches original
    test_utils::expectFilesEqual(test_file.path(), download_path);
}

TEST_F(FullSystemTest, LargeFileMultipleChunks) {
    // Create large test file (3MB = 3 chunks)
    const size_t file_size = 3 * 1024 * 1024;
    auto large_data = test_utils::generateRandomData(file_size);
    
    test_utils::TempFile test_file;
    std::ofstream out(test_file.path(), std::ios::binary);
    out.write(large_data.data(), large_data.size());
    out.close();
    
    // Upload large file
    client_->UploadFile(test_file.path());
    
    // Verify multiple chunks were created
    int chunk_count = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(datanode_temp_->path())) {
        if (entry.path().extension() == ".chunk") {
            chunk_count++;
        }
    }
    EXPECT_EQ(chunk_count, 3) << "Expected 3 chunks for 3MB file";
    
    // Download and verify
    std::string download_path = test_file.path() + ".downloaded";
    client_->DownloadFile(std::filesystem::path(test_file.path()).filename());
    
    test_utils::expectFilesEqual(test_file.path(), download_path);
}

TEST_F(FullSystemTest, EmptyFileHandling) {
    // Create empty file
    test_utils::TempFile empty_file("");
    
    // Upload empty file
    client_->UploadFile(empty_file.path());
    
    // Download and verify
    std::string download_path = empty_file.path() + ".downloaded";
    client_->DownloadFile(std::filesystem::path(empty_file.path()).filename());
    
    // Verify both files are empty
    EXPECT_EQ(std::filesystem::file_size(empty_file.path()), 0);
    EXPECT_EQ(std::filesystem::file_size(download_path), 0);
}

TEST_F(FullSystemTest, BinaryFileHandling) {
    // Create binary file with all possible byte values
    std::vector<char> binary_data;
    for (int i = 0; i < 256; ++i) {
        binary_data.push_back(static_cast<char>(i));
    }
    
    // Repeat pattern to make it larger than 1 chunk
    std::vector<char> large_binary;
    for (int rep = 0; rep < 5000; ++rep) {  // 5000 * 256 = ~1.2MB
        large_binary.insert(large_binary.end(), binary_data.begin(), binary_data.end());
    }
    
    test_utils::TempFile binary_file;
    std::ofstream out(binary_file.path(), std::ios::binary);
    out.write(large_binary.data(), large_binary.size());
    out.close();
    
    // Upload and download
    client_->UploadFile(binary_file.path());
    
    std::string download_path = binary_file.path() + ".downloaded";
    client_->DownloadFile(std::filesystem::path(binary_file.path()).filename());
    
    // Verify byte-for-byte identical
    test_utils::expectFilesEqual(binary_file.path(), download_path);
}

TEST_F(FullSystemTest, MultipleFilesSequential) {
    // Create multiple test files
    std::vector<std::unique_ptr<test_utils::TempFile>> files;
    std::vector<std::string> contents = {
        "File 1: Short content",
        "File 2: Medium length content with some special characters: !@#$%^&*()",
        "File 3: " + std::string(5000, 'A'), // Large file with repeated character
        "",  // Empty file
        "File 5: Final test file"
    };
    
    // Create and upload all files
    for (size_t i = 0; i < contents.size(); ++i) {
        auto file = std::make_unique<test_utils::TempFile>(contents[i]);
        client_->UploadFile(file->path());
        files.push_back(std::move(file));
    }
    
    // Download and verify all files
    for (size_t i = 0; i < files.size(); ++i) {
        std::string download_path = files[i]->path() + ".downloaded";
        client_->DownloadFile(std::filesystem::path(files[i]->path()).filename());
        
        test_utils::expectFilesEqual(files[i]->path(), download_path);
    }
}

TEST_F(FullSystemTest, FileOverwrite) {
    // Create initial file
    std::string initial_content = "Initial content";
    test_utils::TempFile test_file(initial_content);
    std::string filename = std::filesystem::path(test_file.path()).filename();
    
    // Upload initial version
    client_->UploadFile(test_file.path());
    
    // Download and verify initial version
    std::string download_path1 = test_file.path() + ".download1";
    client_->DownloadFile(filename);
    test_utils::expectFilesEqual(test_file.path(), download_path1);
    
    // Create updated file with different content
    std::string updated_content = "Updated content that is much longer than the original";
    test_file.write(updated_content);
    
    // Upload updated version (should overwrite)
    client_->UploadFile(test_file.path());
    
    // Download and verify updated version
    std::string download_path2 = test_file.path() + ".download2";
    client_->DownloadFile(filename);
    test_utils::expectFilesEqual(test_file.path(), download_path2);
    
    // Verify content is actually different
    std::ifstream f1(download_path1);
    std::ifstream f2(download_path2);
    std::string content1((std::istreambuf_iterator<char>(f1)), std::istreambuf_iterator<char>());
    std::string content2((std::istreambuf_iterator<char>(f2)), std::istreambuf_iterator<char>());
    
    EXPECT_NE(content1, content2);
    EXPECT_EQ(content2, updated_content);
}

TEST_F(FullSystemTest, NonExistentFileDownload) {
    // Try to download a file that doesn't exist
    // This should fail gracefully without crashing
    
    // Note: The actual behavior depends on client implementation
    // In a real system, this should either:
    // 1. Throw an exception
    // 2. Return an error status
    // 3. Create an empty file
    // 
    // For this test, we'll just verify it doesn't crash
    EXPECT_NO_THROW({
        client_->DownloadFile("nonexistent_file.txt");
    });
}

TEST_F(FullSystemTest, StressTestManySmallFiles) {
    const int num_files = 50;
    std::vector<std::unique_ptr<test_utils::TempFile>> files;
    
    test_utils::Timer timer;
    
    // Create and upload many small files
    for (int i = 0; i < num_files; ++i) {
        std::string content = "File " + std::to_string(i) + ": " + 
                             test_utils::generateRandomString(100 + (i % 900)); // 100-1000 chars
        
        auto file = std::make_unique<test_utils::TempFile>(content);
        client_->UploadFile(file->path());
        files.push_back(std::move(file));
        
        if ((i + 1) % 10 == 0) {
            std::cout << "Uploaded " << (i + 1) << "/" << num_files << " files" << std::endl;
        }
    }
    
    double upload_time = timer.elapsedSeconds();
    std::cout << "Upload completed in " << upload_time << " seconds" << std::endl;
    
    // Download all files and verify
    timer.reset();
    for (int i = 0; i < num_files; ++i) {
        std::string filename = std::filesystem::path(files[i]->path()).filename();
        std::string download_path = files[i]->path() + ".downloaded";
        
        client_->DownloadFile(filename);
        test_utils::expectFilesEqual(files[i]->path(), download_path);
        
        if ((i + 1) % 10 == 0) {
            std::cout << "Downloaded " << (i + 1) << "/" << num_files << " files" << std::endl;
        }
    }
    
    double download_time = timer.elapsedSeconds();
    std::cout << "Download completed in " << download_time << " seconds" << std::endl;
    
    // Performance expectations (these are rough estimates and may need adjustment)
    EXPECT_LT(upload_time, 30.0) << "Upload took too long: " << upload_time << " seconds";
    EXPECT_LT(download_time, 30.0) << "Download took too long: " << download_time << " seconds";
}

TEST_F(FullSystemTest, DataIntegrityAfterRestart) {
    // Create test file
    auto test_data = test_utils::generateRandomData(2 * 1024 * 1024); // 2MB
    test_utils::TempFile test_file;
    std::ofstream out(test_file.path(), std::ios::binary);
    out.write(test_data.data(), test_data.size());
    out.close();
    
    // Upload file
    std::string filename = std::filesystem::path(test_file.path()).filename();
    client_->UploadFile(test_file.path());
    
    // Stop and restart DataNode (MetaServer stays running)
    datanode_->stop();
    
    // Wait a moment
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Restart DataNode with same storage
    datanode_ = std::make_unique<test_utils::TestDataNode>(
        test_utils::createTestAddress(),
        metaserver_->address(),
        datanode_temp_->path()
    );
    ASSERT_TRUE(datanode_->start());
    
    // Give time for registration and heartbeat
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Download file and verify it's still intact
    std::string download_path = test_file.path() + ".after_restart";
    client_->DownloadFile(filename);
    
    test_utils::expectFilesEqual(test_file.path(), download_path);
}

TEST_F(FullSystemTest, SystemResourceUsage) {
    // This test monitors basic resource usage patterns
    // In a production system, you might want more sophisticated monitoring
    
    const int num_operations = 20;
    std::vector<std::unique_ptr<test_utils::TempFile>> files;
    
    // Create files of varying sizes
    for (int i = 0; i < num_operations; ++i) {
        size_t file_size = (i + 1) * 100 * 1024; // 100KB to 2MB
        auto data = test_utils::generateRandomData(file_size);
        
        auto file = std::make_unique<test_utils::TempFile>();
        std::ofstream out(file->path(), std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
        
        files.push_back(std::move(file));
    }
    
    test_utils::Timer timer;
    
    // Upload all files
    for (const auto& file : files) {
        client_->UploadFile(file->path());
    }
    
    // Download all files
    for (const auto& file : files) {
        std::string filename = std::filesystem::path(file->path()).filename();
        client_->DownloadFile(filename);
    }
    
    double total_time = timer.elapsedSeconds();
    
    // Calculate total data processed
    size_t total_bytes = 0;
    for (const auto& file : files) {
        total_bytes += file->size();
    }
    
    double throughput_mbps = (total_bytes * 2 / (1024.0 * 1024.0)) / total_time; // *2 for upload+download
    
    std::cout << "Processed " << total_bytes / (1024 * 1024) << " MB in " 
              << total_time << " seconds" << std::endl;
    std::cout << "Average throughput: " << throughput_mbps << " MB/s" << std::endl;
    
    // Basic performance expectation (adjust based on system capabilities)
    EXPECT_GT(throughput_mbps, 0.1) << "Throughput too low: " << throughput_mbps << " MB/s";
}