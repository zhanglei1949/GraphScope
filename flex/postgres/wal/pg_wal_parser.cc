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

#include "flex/postgres/wal/pg_wal_parser.h"
#include "flex/engines/graph_db/database/wal_writer_factory.h"

namespace gs {

static constexpr size_t MAX_WALS_NUM = 134217728;

void PGWalParser::Init() {
  WalWriterFactory::RegisterWalParser(
      "postgres", static_cast<WalWriterFactory::wal_parser_initializer_t>(
                      &PGWalParser::Make));
}

PGWalParser::PGWalParser(const std::string& wal_dir)
    : insert_wal_list_(NULL), insert_wal_list_size_(0) {}

PGWalParser::~PGWalParser() {}

uint32_t PGWalParser::last_ts() const { return last_ts_; }

const WalContentUnit& PGWalParser::get_insert_wal(uint32_t ts) const {
  return insert_wal_list_[ts];
}

const std::vector<UpdateWalUnit>& PGWalParser::update_wals() const {
  return update_wal_list_;
}

const bool PGWalParser::registered_ = WalWriterFactory::RegisterWalParser(
    "postgres", static_cast<WalWriterFactory::wal_parser_initializer_t>(
                    &PGWalParser::Make));

struct CustomHash {
  size_t operator()(const std::pair<uint32_t, uint32_t>& key) const {
    uint64_t k = key.first;
    k = (k << 32) | key.second;
    return std::hash<uint64_t>{}(k);
  }
};

}  // namespace gs