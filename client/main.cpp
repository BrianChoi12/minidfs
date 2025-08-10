#include <string> 
#include <iostream>
#include <vector>
#include "mini_dfs_client.hpp"
#include "dfs.grpc.pb.h"
#include <grpcpp/grpcpp.h>

std::vector<std::string> ParseCommand(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string s;
    while (iss >> s) {
        tokens.push_back(s);
    }
    return tokens;
}

void RunClient(const std::string& address) {
    std::shared_ptr<grpc::ChannelInterface> channel{
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials())
    };
    
    MiniDfsClient client{channel}; 

    std::cout << "MiniDFS++ Client Started\n";
    std::cout << "Commands:\n";
    std::cout << "  upload <filename>\n";
    std::cout << "  download <filename>\n";
    std::cout << "  exit\n";

    std::string line;
    while (true) {
        std::cout << "> ";
        std::getline(std::cin, line);
        auto tokens = ParseCommand(line);
        if (tokens.empty()) continue;

        const std::string& cmd = tokens[0];

        if (cmd == "exit") {
            break;
        } else if (cmd == "upload" && tokens.size() == 2) {
            client.UploadFile(tokens[1]);
        } else if (cmd == "download" && tokens.size() == 2) {
            client.DownloadFile(tokens[1]);
        } else {
            std::cout << "[ERROR] Invalid command.\n";
        }
    }
}

int main() {
    RunClient("0.0.0.0:50051");
}
