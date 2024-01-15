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
        auto intermediate = std::vector<KeyVal>();
        auto output_string = std::string();
        for (auto i = 0; i < files.size(); i++) {
            auto res_lookup = chfs_client->lookup(1, files[i]);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            auto content = std::string(char_vec.begin(), char_vec.end());
            auto map_res = Map(content);
            intermediate.insert(intermediate.end(), map_res.begin(), map_res.end());
        }
        sort(intermediate.begin(), intermediate.end(), [](KeyVal const &a, KeyVal const &b) {
            return a.key < b.key;
        });
        
        for(auto i = (unsigned int)0; i < intermediate.size(); ) {
            auto j = i + 1;
            for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key; )
                j++;
            auto values = std::vector<std::string>();
            for (auto k = i; k < j; k++) {
                values.emplace_back(intermediate[k].val);
            }
            auto output = Reduce(intermediate[i].key, values);
            printf("%s %s\n", intermediate[i].key.c_str(), output.data());
            auto sub_string = intermediate[i].key + " " + output.data() + '\n';
            output_string.append(sub_string);
            i = j;
        }
        auto vec = std::vector<uint8_t>(output_string.begin(), output_string.end());
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        auto inode_id = res_lookup.unwrap();
        chfs_client->write_file(inode_id, 0, vec);
        return;
    }
}