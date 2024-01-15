#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {

    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        // auto inode_id_wrap = chfs_client->lookup(1, outPutFile);
        // auto inode_id = chfs::inode_id_t(0);
        // if (inode_id_wrap.is_ok())
        //     inode_id = inode_id_wrap.unwrap();
        
        
    }
}