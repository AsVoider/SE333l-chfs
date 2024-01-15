#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce {
Worker::Worker(MR_CoordinatorConfig config) {
  mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port,
                                                true);
  outPutFile = config.resultFile;
  chfs_client = config.client;
  work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
  // Lab4: Your code goes here (Optional).
}

void Worker::doMap(int index, const std::string& filename) {
  auto map_file = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "map_" + std::to_string(index));
  assert(map_file.is_ok());
  auto node_id = map_file.unwrap();
  auto look_res = chfs_client->lookup(1, filename);
  assert(look_res.is_ok());
  auto file_inode = look_res.unwrap();
  auto attr = chfs_client->get_type_attr(file_inode);
  auto vec = chfs_client->read_file(file_inode, 0, attr.unwrap().second.size).unwrap();
  auto content = std::string(vec.begin(), vec.end());
  auto map_res = Map(content);
  auto ss = std::stringstream();
  for (auto &res : map_res) {
    ss << res.key << ' ' << res.val << ' ';
  }
  content = ss.str();
  vec = std::vector<uint8_t>(content.begin(), content.end());
  chfs_client->write_file(node_id, 0, vec);
}

void Worker::doReduce(int index, int nfiles) {
  // Lab4: Your code goes here.
  auto reduces = std::vector<KeyVal>();
  auto output_string = std::string();
  for (auto i = 0; i < nfiles; i++) {
    auto file_name = "map_" + std::to_string(i);
    auto node_id = chfs_client->lookup(1, file_name).unwrap();
    auto attr = chfs_client->get_type_attr(node_id);
    auto content = chfs_client->read_file(node_id, 0, attr.unwrap().second.size).unwrap();
    auto str = std::string(content.begin(), content.end());
    auto ss = std::stringstream(str);
    while(ss) {
      std::string k, v;
      ss >> k >> v;
      if (k.empty() || k.front() == '\0' || v.empty() || v.front() == '\0')
        break;
      reduces.emplace_back(KeyVal(k, v));
    }   
  }
  sort(reduces.begin(), reduces.end(), [](KeyVal const &a, KeyVal const &b) {
    return a.key < b.key;
  });
  for(auto i = (unsigned int)0; i < reduces.size(); ) {
    auto j = i + 1;
    for (; j < reduces.size() && reduces[j].key == reduces[i].key; )
      j++;
    auto values = std::vector<std::string>();
    for (auto k = i; k < j; k++) {
      values.emplace_back(reduces[k].val);
    }
    auto output = Reduce(reduces[i].key, values);
    printf("%s %s\n", reduces[i].key.c_str(), output.data());
    auto sub_string = reduces[i].key + " " + output.data() + '\n';
    output_string.append(sub_string);
    i = j;
  }
  auto vect = std::vector<uint8_t>(output_string.begin(), output_string.end());
  auto res_lookup = chfs_client->lookup(1, outPutFile);
  auto inode_id = res_lookup.unwrap();
  chfs_client->write_file(inode_id, 0, vect);
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
  // Lab4: Your code goes here.
  mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
}

void Worker::stop() {
  shouldStop = true;
  work_thread->join();
}

void Worker::doWork() {
  int i = 0;
  while (!shouldStop) {
      // Lab4: Your code goes here.
    auto tsk = mr_client->call(ASK_TASK, i);
    assert(tsk.is_ok());
    // index : type : num_reducer : filenum : filename
    auto info = tsk.unwrap()->as<Info>();
    this->n_reducer = info.n_reducer;
    if (info.type == 0) {
      if (info.index == -1)
        return;
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }
    if (info.type == 1) {
      doMap(info.index, info.file);
      doSubmit(MAP, info.index);
      continue;
    }
    if (info.type == 2) {
      doReduce(info.index, info.nfiles);
      doSubmit(REDUCE, info.index);
    }
  }
}
}