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

#include "flex/engines/graph_db/database/wal/pg_wal_writer.h"
#include "flex/engines/graph_db/database/wal/wal_writer_factory.h"

#include "flex/engines/graph_db/database/wal/pg/pg_wal_utils.h"

#include <chrono>
#include <filesystem>

namespace gs {

std::unique_ptr<IWalWriter> PGWalWriter::Make() {
  return std::unique_ptr<IWalWriter>(new PGWalWriter());
}

void PGWalWriter::open(const std::string& prefix, int thread_id) {
  path_ = prefix;
  thread_id_ = thread_id;
  StartPostMaster(path_.c_str());
  /*
   * Start the postmaster in this process to handle the wal writing. It will
   * create several subprocesses.
   *
   * 1. WalWriter: It will write the wal to the disk.
   *
   * The wal will be written to the directory specified by the path_.
   */
}

void PGWalWriter::close() { StopPostMaster(); }

bool PGWalWriter::append(const char* data, size_t length) {
  return WriteWal(data, length);
}

const bool PGWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "local", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                 &PGWalWriter::Make));

}  // namespace gs