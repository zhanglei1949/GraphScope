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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_WAL_WRITER_FACTORY_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_WAL_WRITER_FACTORY_H_

#include <memory>
#include <unordered_map>
#include "flex/engines/graph_db/database/wal.h"
#include "flex/engines/graph_db/database/wal_parser.h"

namespace gs {

/**
 * @brief WalWriterFactory is a factory class to create IWalWriter.
 * A customized wal writer should be registered before using, with a unique
 * wal_writer_type string.
 */
class WalWriterFactory {
 public:
  using wal_writer_initializer_t = std::unique_ptr<IWalWriter> (*)();
  using wal_parser_initializer_t =
      std::unique_ptr<IWalParser> (*)(const std::string& wal_dir);

  static void Init();

  static void Finalize();

  static std::unique_ptr<IWalWriter> CreateWalWriter(
      const std::string& wal_writer_type);

  static std::unique_ptr<IWalParser> CreateWalParser(
      const std::string& wal_writer_type, const std::string& wal_dir);

  static bool RegisterWalWriter(const std::string& wal_writer_type,
                                wal_writer_initializer_t initializer);

  static bool RegisterWalParser(const std::string& wal_writer_type,
                                wal_parser_initializer_t initializer);

 private:
  static std::unordered_map<std::string, wal_writer_initializer_t>&
  getKnownWalWriters();
  static std::unordered_map<std::string, wal_parser_initializer_t>&
  getKnownWalParsers();
};
}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_WAL_WRITER_FACTORY_H_