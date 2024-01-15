#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
Info Coordinator::askTask(int i) {
  // Lab4 : Your code goes here.
  // Free to change the type of return value.
  if (reduce_tasks > 0) {
    return Info(-1, 0, -1, num_reducer, "");
  }
  if (map_tasks < files.size()) {
    auto index = map_tasks++;
    return Info(index, 1, files.size(), num_reducer, files[index]);
  }
  if (!is_mapfinished)
    return Info(0, 0, -1, num_reducer, "");
  reduce_tasks++;
  return Info(0, 2, files.size(), num_reducer, "");
}

int Coordinator::submitTask(int taskType, int index) {
  // Lab4 : Your code goes here.
  std::unique_lock<std::mutex> lock(mtx);
  if (taskType == 1) {
    this->num_donemap++;
    if (this->num_donemap >= files.size())
      this->is_mapfinished = true;
  }
  if (taskType == 2) {
    this->isFinished = true;
  }
  return 0;
}

// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
bool Coordinator::Done() {
  std::unique_lock uniqueLock(this->mtx);
  return this->isFinished;
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
// use Only one Reduce Worker
Coordinator::Coordinator(MR_CoordinatorConfig config,
                         const std::vector<std::string>& files, int nReduce) {
  this->files = files;
  this->isFinished = false;
  this->num_reducer = nReduce;
  this->num_donemap = 0;
  this->is_mapfinished = false;
  this->map_tasks = 0;
  this->reduce_tasks = 0;
  // Lab4: Your code goes here (Optional).

  rpc_server = std::make_unique<
    chfs::RpcServer>(config.ip_address, config.port);
  rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
  rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) {
    return this->submitTask(taskType, index);
  });
  rpc_server->run(true, 1);
}
}
