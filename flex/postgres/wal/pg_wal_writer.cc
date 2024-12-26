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

#include "flex/postgres/wal/pg_wal_writer.h"
#include "flex/engines/graph_db/database/wal_writer_factory.h"

#include "flex/postgres/wal/pg_wal_utils.h"

#include <chrono>
#include <filesystem>

namespace gs {

void PGWalWriter::Init() {
  WalWriterFactory::RegisterWalWriter(
      "postgres", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                      &PGWalWriter::Make));
}

std::unique_ptr<IWalWriter> PGWalWriter::Make() {
  LOG(INFO) << "Create PGWalWriter";
  return std::unique_ptr<IWalWriter>(new PGWalWriter());
}

void PGWalWriter::open(const std::string& prefix, int thread_id) {
  path_ = prefix;
  thread_id_ = thread_id;
  LOG(INFO) << "Open PGWalWriter";
  // StartPostMaster(path_);
  /*
   * Start the postmaster in this process to handle the wal writing. It will
   * create several subprocesses.
   *
   * 1. WalWriter: It will write the wal to the disk.
   *
   * The wal will be written to the directory specified by the path_.
   */

  // Read the parsed wals from the socket.
  // ReadWalsFromSocket();
}

void PGWalWriter::close() {LOG(INFO) << "Close PGWalWriter: " << thread_id_;}

bool PGWalWriter::append(const char* data, size_t length) {
  LOG(INFO) << "Write wal content: " << std::string(data, length);
  return WriteWal(data, length);
}

const bool PGWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "postgres", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                    &PGWalWriter::Make));

}  // namespace gs