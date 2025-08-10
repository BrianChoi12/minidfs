#include <gtest/gtest.h>
#include "../utils/test_utils.hpp"
#include "mini_dfs_client.hpp"
#include <thread>
#include <future>
#include <atomic>

class PerformanceTest : public ::testing::Test {
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
        
        // Give DataNode time to register
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
    
    struct PerformanceMetrics {
        double throughput_mbps = 0.0;
        double latency_ms = 0.0;
        double operations_per_second = 0.0;
        size_t total_bytes = 0;
        double total_time_seconds = 0.0;
    };
    
    PerformanceMetrics measureUploadPerformance(const std::vector<std::unique_ptr<test_utils::TempFile>>& files) {
        test_utils::Timer timer;
        size_t total_bytes = 0;
        
        for (const auto& file : files) {
            client_->UploadFile(file->path());
            total_bytes += file->size();
        }
        
        double elapsed = timer.elapsedSeconds();
        
        PerformanceMetrics metrics;
        metrics.total_bytes = total_bytes;
        metrics.total_time_seconds = elapsed;
        metrics.throughput_mbps = (total_bytes / (1024.0 * 1024.0)) / elapsed;
        metrics.latency_ms = (elapsed / files.size()) * 1000.0;
        metrics.operations_per_second = files.size() / elapsed;
        
        return metrics;
    }
    
    PerformanceMetrics measureDownloadPerformance(const std::vector<std::string>& filenames) {
        test_utils::Timer timer;
        size_t total_bytes = 0;
        
        for (const auto& filename : filenames) {
            client_->DownloadFile(filename);
            
            // Calculate file size from downloaded file
            std::string downloaded_path = filename;
            if (std::filesystem::exists(downloaded_path)) {
                total_bytes += std::filesystem::file_size(downloaded_path);
            }
        }
        
        double elapsed = timer.elapsedSeconds();
        
        PerformanceMetrics metrics;
        metrics.total_bytes = total_bytes;
        metrics.total_time_seconds = elapsed;
        metrics.throughput_mbps = (total_bytes / (1024.0 * 1024.0)) / elapsed;
        metrics.latency_ms = (elapsed / filenames.size()) * 1000.0;
        metrics.operations_per_second = filenames.size() / elapsed;
        
        return metrics;
    }
    
    void printMetrics(const std::string& test_name, const PerformanceMetrics& metrics) {
        std::cout << "\n" << test_name << " Performance Metrics:\n";
        std::cout << "  Total data: " << metrics.total_bytes / (1024 * 1024) << " MB\n";
        std::cout << "  Total time: " << metrics.total_time_seconds << " seconds\n";
        std::cout << "  Throughput: " << metrics.throughput_mbps << " MB/s\n";
        std::cout << "  Average latency: " << metrics.latency_ms << " ms\n";
        std::cout << "  Operations per second: " << metrics.operations_per_second << "\n";
    }
    
    std::unique_ptr<test_utils::TempDirectory> metaserver_temp_;
    std::unique_ptr<test_utils::TempDirectory> datanode_temp_;
    std::unique_ptr<test_utils::TestMetaServer> metaserver_;
    std::unique_ptr<test_utils::TestDataNode> datanode_;
    std::unique_ptr<MiniDfsClient> client_;
};

TEST_F(PerformanceTest, SmallFilesThroughput) {
    // Test many small files (typical web scenario)
    const int num_files = 100;
    const size_t file_size = 1024; // 1KB files
    
    std::vector<std::unique_ptr<test_utils::TempFile>> files;
    std::vector<std::string> filenames;
    
    // Generate test files
    for (int i = 0; i < num_files; ++i) {
        auto data = test_utils::generateRandomData(file_size);
        auto file = std::make_unique<test_utils::TempFile>();
        
        std::ofstream out(file->path(), std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
        
        filenames.push_back(std::filesystem::path(file->path()).filename());
        files.push_back(std::move(file));
    }
    
    // Measure upload performance
    auto upload_metrics = measureUploadPerformance(files);
    printMetrics("Small Files Upload", upload_metrics);
    
    // Measure download performance
    auto download_metrics = measureDownloadPerformance(filenames);
    printMetrics("Small Files Download", download_metrics);
    
    // Performance expectations for small files
    EXPECT_GT(upload_metrics.operations_per_second, 10.0) << "Upload OPS too low";
    EXPECT_GT(download_metrics.operations_per_second, 10.0) << "Download OPS too low";
    EXPECT_LT(upload_metrics.latency_ms, 1000.0) << "Upload latency too high";
    EXPECT_LT(download_metrics.latency_ms, 1000.0) << "Download latency too high";
}

TEST_F(PerformanceTest, LargeFilesThroughput) {
    // Test fewer large files (typical backup scenario)
    const int num_files = 10;
    const size_t file_size = 5 * 1024 * 1024; // 5MB files
    
    std::vector<std::unique_ptr<test_utils::TempFile>> files;
    std::vector<std::string> filenames;
    
    // Generate test files
    for (int i = 0; i < num_files; ++i) {
        auto data = test_utils::generateRandomData(file_size);
        auto file = std::make_unique<test_utils::TempFile>();
        
        std::ofstream out(file->path(), std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
        
        filenames.push_back(std::filesystem::path(file->path()).filename());
        files.push_back(std::move(file));
    }
    
    // Measure upload performance
    auto upload_metrics = measureUploadPerformance(files);
    printMetrics("Large Files Upload", upload_metrics);
    
    // Measure download performance
    auto download_metrics = measureDownloadPerformance(filenames);
    printMetrics("Large Files Download", download_metrics);
    
    // Performance expectations for large files
    EXPECT_GT(upload_metrics.throughput_mbps, 0.5) << "Upload throughput too low";
    EXPECT_GT(download_metrics.throughput_mbps, 0.5) << "Download throughput too low";
    EXPECT_LT(upload_metrics.latency_ms, 10000.0) << "Upload latency too high";
    EXPECT_LT(download_metrics.latency_ms, 10000.0) << "Download latency too high";
}

TEST_F(PerformanceTest, ConcurrentClientsStress) {
    // Test system under concurrent load
    const int num_clients = 5;
    const int files_per_client = 10;
    const size_t file_size = 512 * 1024; // 512KB files
    
    // Prepare test data
    std::vector<std::vector<std::unique_ptr<test_utils::TempFile>>> client_files(num_clients);
    
    for (int client = 0; client < num_clients; ++client) {
        for (int file = 0; file < files_per_client; ++file) {
            auto data = test_utils::generateRandomData(file_size);
            auto temp_file = std::make_unique<test_utils::TempFile>();
            
            std::ofstream out(temp_file->path(), std::ios::binary);
            out.write(data.data(), data.size());
            out.close();
            
            client_files[client].push_back(std::move(temp_file));
        }
    }
    
    test_utils::Timer total_timer;
    std::atomic<int> completed_uploads{0};
    std::atomic<int> failed_uploads{0};
    
    // Launch concurrent upload clients
    std::vector<std::future<void>> upload_futures;
    
    for (int client = 0; client < num_clients; ++client) {
        auto future = std::async(std::launch::async, [&, client]() {
            // Create separate client for this thread
            auto channel = grpc::CreateChannel(metaserver_->address(), grpc::InsecureChannelCredentials());
            MiniDfsClient thread_client(channel);
            
            for (const auto& file : client_files[client]) {
                try {
                    thread_client.UploadFile(file->path());
                    completed_uploads++;
                } catch (...) {
                    failed_uploads++;
                }
            }
        });
        
        upload_futures.push_back(std::move(future));
    }
    
    // Wait for all uploads to complete
    for (auto& future : upload_futures) {
        future.wait();
    }
    
    double upload_time = total_timer.elapsedSeconds();
    
    // Launch concurrent download clients
    total_timer.reset();
    std::atomic<int> completed_downloads{0};
    std::atomic<int> failed_downloads{0};
    
    std::vector<std::future<void>> download_futures;
    
    for (int client = 0; client < num_clients; ++client) {
        auto future = std::async(std::launch::async, [&, client]() {
            auto channel = grpc::CreateChannel(metaserver_->address(), grpc::InsecureChannelCredentials());
            MiniDfsClient thread_client(channel);
            
            for (const auto& file : client_files[client]) {
                try {
                    std::string filename = std::filesystem::path(file->path()).filename();
                    thread_client.DownloadFile(filename);
                    completed_downloads++;
                } catch (...) {
                    failed_downloads++;
                }
            }
        });
        
        download_futures.push_back(std::move(future));
    }
    
    // Wait for all downloads to complete
    for (auto& future : download_futures) {
        future.wait();
    }
    
    double download_time = total_timer.elapsedSeconds();
    
    // Calculate metrics
    int total_operations = num_clients * files_per_client;
    size_t total_data_mb = (total_operations * file_size) / (1024 * 1024);
    
    std::cout << "\nConcurrent Stress Test Results:\n";
    std::cout << "  Clients: " << num_clients << "\n";
    std::cout << "  Files per client: " << files_per_client << "\n";
    std::cout << "  Total data: " << total_data_mb << " MB\n";
    std::cout << "  Upload time: " << upload_time << " seconds\n";
    std::cout << "  Download time: " << download_time << " seconds\n";
    std::cout << "  Successful uploads: " << completed_uploads << "/" << total_operations << "\n";
    std::cout << "  Successful downloads: " << completed_downloads << "/" << total_operations << "\n";
    std::cout << "  Upload throughput: " << (total_data_mb / upload_time) << " MB/s\n";
    std::cout << "  Download throughput: " << (total_data_mb / download_time) << " MB/s\n";
    
    // Stress test expectations
    EXPECT_EQ(failed_uploads.load(), 0) << "Some uploads failed under concurrent load";
    EXPECT_EQ(failed_downloads.load(), 0) << "Some downloads failed under concurrent load";
    EXPECT_EQ(completed_uploads.load(), total_operations) << "Not all uploads completed";
    EXPECT_EQ(completed_downloads.load(), total_operations) << "Not all downloads completed";
    
    // Performance should degrade gracefully under load but not fail completely
    EXPECT_LT(upload_time, 60.0) << "Upload time under concurrent load too high";
    EXPECT_LT(download_time, 60.0) << "Download time under concurrent load too high";
}

TEST_F(PerformanceTest, MemoryUsagePattern) {
    // Test memory usage with various file sizes
    const std::vector<size_t> file_sizes = {
        1024,           // 1KB
        64 * 1024,      // 64KB
        1024 * 1024,    // 1MB
        5 * 1024 * 1024 // 5MB
    };
    
    for (size_t file_size : file_sizes) {
        auto data = test_utils::generateRandomData(file_size);
        test_utils::TempFile test_file;
        
        std::ofstream out(test_file.path(), std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
        
        // Measure upload
        test_utils::Timer timer;
        client_->UploadFile(test_file.path());
        double upload_time = timer.elapsedSeconds();
        
        // Measure download
        timer.reset();
        std::string filename = std::filesystem::path(test_file.path()).filename();
        client_->DownloadFile(filename);
        double download_time = timer.elapsedSeconds();
        
        std::cout << "File size: " << file_size / 1024 << " KB, "
                  << "Upload: " << upload_time << "s, "
                  << "Download: " << download_time << "s\n";
        
        // Memory usage should scale reasonably with file size
        // (This is a basic test; more sophisticated monitoring would be needed for production)
        EXPECT_LT(upload_time, 30.0) << "Upload time excessive for " << file_size << " byte file";
        EXPECT_LT(download_time, 30.0) << "Download time excessive for " << file_size << " byte file";
    }
}

TEST_F(PerformanceTest, ChunkSizeBoundaryPerformance) {
    // Test files around chunk boundaries (1MB)
    const std::vector<size_t> test_sizes = {
        1024 * 1024 - 1,     // Just under 1MB (1 chunk)
        1024 * 1024,         // Exactly 1MB (1 chunk)
        1024 * 1024 + 1,     // Just over 1MB (2 chunks)
        2 * 1024 * 1024 - 1, // Just under 2MB (2 chunks)
        2 * 1024 * 1024,     // Exactly 2MB (2 chunks)
        2 * 1024 * 1024 + 1  // Just over 2MB (3 chunks)
    };
    
    for (size_t size : test_sizes) {
        auto data = test_utils::generatePatternData(size, "ABCD");
        test_utils::TempFile test_file;
        
        std::ofstream out(test_file.path(), std::ios::binary);
        out.write(data.data(), data.size());
        out.close();
        
        // Test upload/download cycle
        test_utils::Timer timer;
        client_->UploadFile(test_file.path());
        
        std::string filename = std::filesystem::path(test_file.path()).filename();
        client_->DownloadFile(filename);
        double total_time = timer.elapsedSeconds();
        
        // Verify integrity
        std::string download_path = filename;
        if (std::filesystem::exists(download_path)) {
            test_utils::expectFilesEqual(test_file.path(), download_path);
        }
        
        int expected_chunks = (size + 1024 * 1024 - 1) / (1024 * 1024); // Ceiling division
        
        std::cout << "Size: " << size << " bytes (" << expected_chunks << " chunks), "
                  << "Time: " << total_time << "s\n";
        
        // Performance should be predictable based on chunk count
        EXPECT_LT(total_time, 10.0 * expected_chunks) << "Performance degraded for " << size << " byte file";
    }
}

TEST_F(PerformanceTest, SystemStabilityUnderLoad) {
    // Run continuous operations for extended period
    const int duration_seconds = 10; // Keep short for CI
    const size_t file_size = 256 * 1024; // 256KB
    
    test_utils::Timer test_timer;
    std::atomic<int> operations_completed{0};
    std::atomic<int> operations_failed{0};
    std::atomic<bool> stop_flag{false};
    
    // Launch background thread for continuous operations
    auto worker = std::async(std::launch::async, [&]() {
        int file_counter = 0;
        
        while (!stop_flag.load() && test_timer.elapsedSeconds() < duration_seconds) {
            try {
                // Create unique file
                auto data = test_utils::generateRandomData(file_size);
                test_utils::TempFile temp_file;
                
                std::ofstream out(temp_file.path(), std::ios::binary);
                out.write(data.data(), data.size());
                out.close();
                
                // Upload
                client_->UploadFile(temp_file.path());
                
                // Download
                std::string filename = std::filesystem::path(temp_file.path()).filename();
                client_->DownloadFile(filename);
                
                operations_completed++;
                file_counter++;
                
                // Brief pause to avoid overwhelming system
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                
            } catch (...) {
                operations_failed++;
            }
        }
    });
    
    // Wait for test duration
    worker.wait();
    stop_flag = true;
    
    double actual_duration = test_timer.elapsedSeconds();
    double ops_per_second = operations_completed.load() / actual_duration;
    
    std::cout << "\nStability Test Results (" << actual_duration << "s):\n";
    std::cout << "  Operations completed: " << operations_completed << "\n";
    std::cout << "  Operations failed: " << operations_failed << "\n";
    std::cout << "  Operations per second: " << ops_per_second << "\n";
    
    // Stability expectations
    EXPECT_GT(operations_completed.load(), 0) << "No operations completed";
    EXPECT_LT(operations_failed.load(), operations_completed.load() * 0.05) << "Too many failures (>5%)";
    EXPECT_GT(ops_per_second, 0.5) << "Operations per second too low";
}