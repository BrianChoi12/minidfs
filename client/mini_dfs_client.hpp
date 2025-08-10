#include <string>
#include "dfs.grpc.pb.h"

class MiniDfsClient {
private:
    MetaService::Stub theStub;
public: 
    MiniDfsClient(std::shared_ptr<grpc::ChannelInterface> aChannel);

    void UploadFile(const std::string& fileName);

    void DownloadFile(const std::string& fileName); 
};
