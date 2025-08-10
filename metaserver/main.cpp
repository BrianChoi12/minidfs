#include <string>
#include <iostream>
#include "cache.hpp"
#include "manager.hpp"
#include "server.hpp"
#include "dfs.grpc.pb.h"
#include <grpcpp/server_builder.h>
#include <grpcpp/server.h>

using ::grpc::Status;
using ::grpc::ServerContext;
using ::grpc::ServerBuilder;
using ::grpc::Server;

class RPCServiceImpl final : public MetaService::Service {
private:
    Manager* theManager; 
public:  
    RPCServiceImpl(Manager* aManager) : theManager(aManager) {
    }

    Status RegisterDataNode(ServerContext* context, const ::DataNodeInfo* request, ::Ack* response) {
        bool success = theManager->registerDataNode(
            request->address(),
            request->available_space()
        );
        
        response->set_ok(success);
        response->set_message(success ? "DataNode registered successfully" : "Failed to register DataNode");
        
        return Status::OK; 
    }

    Status Heartbeat(ServerContext* context, const ::DataNodeHeartbeat* request, ::HeartbeatResponse* response) {
        // Convert repeated field to vector
        std::vector<std::string> chunks;
        for (const auto& chunk : request->stored_chunk_ids()) {
            chunks.push_back(chunk);
        }
        
        bool success = theManager->updateDataNodeHeartbeat(
            request->address(),
            chunks,
            request->available_space(),
            request->current_load()
        );
        
        response->set_ok(success);
        // In the future, we could add chunks_to_delete for garbage collection
        
        return Status::OK; 
    }

    Status GetFileLocation(ServerContext* context, const ::FileLocationRequest* request, ::FileLocationResponse* response) {
        auto locations = theManager->getFileLocation(request->filename());
        
        if (locations.empty()) {
            response->set_found(false);
            return Status::OK;
        }
        
        response->set_found(true);
        for (const auto& loc : locations) {
            auto* chunk_loc = response->add_chunks();
            chunk_loc->set_chunk_id(loc.chunk_id);
            for (const auto& addr : loc.datanode_addresses) {
                chunk_loc->add_datanode_addresses(addr);
            }
        }
        
        return Status::OK;
    }

    Status AllocateChunkLocation(ServerContext* context, const ::ChunkAllocationRequest* request, ::ChunkLocation* response) {
        auto [chunk_id, datanode_addresses] = theManager->allocateChunkLocation(
            request->filename(),
            request->chunk_index(),
            request->chunk_size()
        );
        
        if (chunk_id.empty()) {
            return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, 
                               "No available DataNode for chunk allocation");
        }
        
        response->set_chunk_id(chunk_id);
        for (const auto& addr : datanode_addresses) {
            response->add_datanode_addresses(addr);
        }
        
        return Status::OK;
    }
};

void RunServer(const std::string& address) {
    Cache cache(1000);  // LRU cache with capacity of 1000 chunks
    Manager manager(&cache); 
    RPCServiceImpl service(&manager); 

    ServerBuilder server_builder;
    server_builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    server_builder.RegisterService(&service);

    std::unique_ptr<Server> server{server_builder.BuildAndStart()}; 
    
    std::cout << "Server listening on " << address << "\n"; 
    server->Wait(); 
}

int main(int argc, char* argv[]) {
    RunServer("0.0.0.0:50051");
    return 0; 
}
