#include "mini_dfs_client.hpp"
#include <fstream>
#include <iostream>
#include <grpcpp/grpcpp.h>
#include <sstream>
#include <iomanip>
#include <filesystem>

constexpr size_t CHUNK_SIZE = 1024 * 1024; // 1 MB Chunks

MiniDfsClient::MiniDfsClient(std::shared_ptr<grpc::ChannelInterface> aChannel) : theStub{aChannel} {}

void MiniDfsClient::UploadFile(const std::string& fileName) {
    std::ifstream file(fileName, std::ios::binary);
    std::vector<std::vector<char>> chunks; 

    if(!file.is_open()) {
        std::cerr << "[ERROR] Cannot open file: " << fileName << "\n"; 
        return;
    }

    // Read file into chunks
    while (!file.eof()) {
        std::vector<char> buffer(CHUNK_SIZE);
        file.read(buffer.data(), CHUNK_SIZE);
        std::streamsize bytesRead = file.gcount();
        buffer.resize(bytesRead);  // trim unused part
        if (bytesRead > 0) {
            chunks.push_back(std::move(buffer));
        }
    }
    file.close();

    std::cout << "[INFO] File split into " << chunks.size() << " chunks\n";

    // Extract just the filename (not the full path) for MetaServer storage
    std::string filename_only = std::filesystem::path(fileName).filename().string();

    // Handle empty files by registering them with MetaServer
    if (chunks.empty()) {
        ChunkAllocationRequest allocRequest;
        allocRequest.set_filename(filename_only);
        allocRequest.set_chunk_index(0);
        allocRequest.set_chunk_size(0);

        ChunkLocation chunkLocation;
        grpc::ClientContext allocContext;
        grpc::Status allocStatus = theStub.AllocateChunkLocation(&allocContext, allocRequest, &chunkLocation);

        if (!allocStatus.ok()) {
            std::cerr << "[ERROR] Failed to register empty file with MetaServer: " 
                      << allocStatus.error_message() << "\n";
            return;
        }

        std::cout << "[SUCCESS] Empty file registered with MetaServer\n";
    }

    // For each chunk, request allocation from MetaServer and upload to DataNode
    for (size_t i = 0; i < chunks.size(); ++i) {
        // Request chunk allocation from MetaServer
        ChunkAllocationRequest allocRequest;
        allocRequest.set_filename(filename_only);
        allocRequest.set_chunk_index(i);
        allocRequest.set_chunk_size(chunks[i].size());

        ChunkLocation chunkLocation;
        grpc::ClientContext allocContext;
        grpc::Status allocStatus = theStub.AllocateChunkLocation(&allocContext, allocRequest, &chunkLocation);

        if (!allocStatus.ok()) {
            std::cerr << "[ERROR] Failed to allocate chunk " << i << " from MetaServer: " 
                      << allocStatus.error_message() << "\n";
            return;
        }

        if (chunkLocation.datanode_addresses_size() == 0) {
            std::cerr << "[ERROR] No DataNode assigned for chunk " << i << "\n";
            return;
        }

        // Try to store chunk to assigned DataNodes (with retry logic)
        bool stored = false;
        for (const std::string& datanodeAddr : chunkLocation.datanode_addresses()) {
            std::cout << "[INFO] Storing chunk " << chunkLocation.chunk_id() 
                      << " (index " << i << ") to DataNode: " << datanodeAddr << "\n";

            // Create a channel to the DataNode
            auto channel = grpc::CreateChannel(datanodeAddr, grpc::InsecureChannelCredentials());
            DataNodeService::Stub datanodeStub(channel);

            // Prepare chunk data with MetaServer-assigned chunk_id
            ChunkData chunkData;
            chunkData.set_chunk_id(chunkLocation.chunk_id());
            chunkData.set_data(chunks[i].data(), chunks[i].size());

            // Send chunk to DataNode
            Ack ack;
            grpc::ClientContext dnContext;
            grpc::Status dnStatus = datanodeStub.StoreChunk(&dnContext, chunkData, &ack);

            if (dnStatus.ok() && ack.ok()) {
                std::cout << "[SUCCESS] Chunk " << chunkLocation.chunk_id() 
                          << " stored successfully\n";
                stored = true;
                break;
            } else {
                std::cerr << "[WARNING] Failed to store chunk to " << datanodeAddr 
                          << ": " << (dnStatus.ok() ? ack.message() : dnStatus.error_message()) << "\n";
            }
        }

        if (!stored) {
            std::cerr << "[ERROR] Could not store chunk " << i << " to any DataNode\n";
            return;
        }
    }

    std::cout << "[SUCCESS] Upload completed for file: " << fileName << "\n";
}

void MiniDfsClient::DownloadFile(const std::string& fileName) {
    // Request file location from MetaServer
    FileLocationRequest request;
    request.set_filename(fileName);

    FileLocationResponse response;
    grpc::ClientContext context;
    grpc::Status status = theStub.GetFileLocation(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "[ERROR] Failed to get file location from MetaServer: " 
                  << status.error_message() << "\n";
        return;
    }

    if (!response.found()) {
        std::cerr << "[ERROR] File not found: " << fileName << "\n";
        return;
    }

    // Handle empty files (0 chunks is valid)
    if (response.chunks_size() == 0) {
        std::cout << "[INFO] Downloading empty file: " << fileName << "\n";
        
        // Create empty output file
        std::ofstream outFile(fileName, std::ios::binary);
        if (!outFile.is_open()) {
            std::cerr << "[ERROR] Cannot create output file: " << fileName << "\n";
            return;
        }
        outFile.close();
        
        std::cout << "[SUCCESS] Download completed for empty file: " << fileName << "\n";
        return;
    }

    std::cout << "[INFO] Downloading " << response.chunks_size() << " chunks for file: " << fileName << "\n";

    // Open output file
    std::ofstream outFile(fileName, std::ios::binary);
    if (!outFile.is_open()) {
        std::cerr << "[ERROR] Cannot create output file: " << fileName << "\n";
        return;
    }

    // Download each chunk from DataNodes
    for (const ChunkLocation& chunkLoc : response.chunks()) {
        if (chunkLoc.datanode_addresses_size() == 0) {
            std::cerr << "[ERROR] No DataNode available for chunk " << chunkLoc.chunk_id() << "\n";
            outFile.close();
            std::remove(fileName.c_str());
            return;
        }

        bool chunkRetrieved = false;
        
        // Try each DataNode address until successful
        for (const std::string& datanodeAddr : chunkLoc.datanode_addresses()) {
            std::cout << "[INFO] Retrieving chunk " << chunkLoc.chunk_id() 
                      << " from DataNode: " << datanodeAddr << "\n";

            // Create a channel to the DataNode
            auto channel = grpc::CreateChannel(datanodeAddr, grpc::InsecureChannelCredentials());
            DataNodeService::Stub datanodeStub(channel);

            // Request chunk
            ChunkRequest chunkRequest;
            chunkRequest.set_chunk_id(chunkLoc.chunk_id());

            ChunkData chunkData;
            grpc::ClientContext dnContext;
            grpc::Status dnStatus = datanodeStub.ReadChunk(&dnContext, chunkRequest, &chunkData);

            if (dnStatus.ok() && chunkData.data().size() > 0) {
                // Write chunk to file
                outFile.write(chunkData.data().c_str(), chunkData.data().size());
                std::cout << "[SUCCESS] Retrieved chunk " << chunkLoc.chunk_id() 
                          << " (" << chunkData.data().size() << " bytes)\n";
                chunkRetrieved = true;
                break;
            } else {
                std::cerr << "[WARNING] Failed to retrieve chunk from " << datanodeAddr 
                          << ": " << dnStatus.error_message() << "\n";
            }
        }

        if (!chunkRetrieved) {
            std::cerr << "[ERROR] Could not retrieve chunk " << chunkLoc.chunk_id() 
                      << " from any DataNode\n";
            outFile.close();
            std::remove(fileName.c_str());  // Remove incomplete file
            return;
        }
    }

    outFile.close();
    std::cout << "[SUCCESS] Download completed for file: " << fileName << "\n";
}
