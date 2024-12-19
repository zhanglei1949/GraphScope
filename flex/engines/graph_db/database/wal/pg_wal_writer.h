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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_WRITER_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_WRITER_H_

#include <memory>
#include <unordered_map>
#include "flex/engines/graph_db/database/wal/wal.h"
#include "flex/engines/graph_db/database/wal/wal_writer_factory.h"

namespace gs {

/**
 * @brief PGWalWriter use postgres functions to write wal.
 */
class PGWalWriter : public IWalWriter {
 public:
  static std::unique_ptr<IWalWriter> Make();

  PGWalWriter() = default;

  void open(const std::string& path, int thread_id) override;
  void close() override;
  bool append(const char* data, size_t length) override;

 private:
  std::string path_;  // Path to the pg wal directory
  int thread_id_;  // The thread_id does not have any effect on the PGWalWriter.

  static const bool registered_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_WRITER_H_