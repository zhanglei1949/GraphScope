/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GRAPHSCOPE_DATABASE_WAL_WAL_H_
#define GRAPHSCOPE_DATABASE_WAL_WAL_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>

#include "glog/logging.h"

namespace gs {

struct WalHeader {
  uint32_t timestamp;
  uint8_t type : 1;
  int32_t length : 31;
};

struct WalContentUnit {
  char* ptr{NULL};
  size_t size{0};
};

struct UpdateWalUnit {
  uint32_t timestamp{0};
  char* ptr{NULL};
  size_t size{0};
};

/**
 * The interface of wal writer.
 */
class IWalWriter {
 public:
  virtual ~IWalWriter() {}
  /**
   * Open a wal file. In our design, each thread has its own wal file.
   * The uri could be a file_path or a remote connection string.
   */
  virtual void open(const std::string& uri, int thread_id) = 0;

  /**
   * Close the wal writer. If a remote connection is hold by the wal writer,
   * it should be closed.
   */
  virtual void close() = 0;

  /**
   * Append data to the wal file.
   */
  virtual bool append(const char* data, size_t length) = 0;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_WAL_WAL_H_
