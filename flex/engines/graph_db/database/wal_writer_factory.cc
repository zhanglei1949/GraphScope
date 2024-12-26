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

#include "flex/engines/graph_db/database/wal_writer_factory.h"
#include <dlfcn.h>
#include <memory>
#include <utility>

#include <boost/algorithm/string.hpp>

namespace gs {

void WalWriterFactory::Init() {
  // get env FLEX_OTHER_LOADERS
  if (getenv("FLEX_OTHER_WAL_WRITERS")) {
    auto other_loaders = getenv("FLEX_OTHER_WAL_WRITERS");
    std::vector<std::string> adaptors;
    ::boost::split(adaptors, other_loaders,
                   ::boost::is_any_of(std::string(1, ':')));
    for (auto const& adaptor : adaptors) {
      if (!adaptor.empty()) {
        if (dlopen(adaptor.c_str(), RTLD_GLOBAL | RTLD_NOW) == nullptr) {
          LOG(WARNING) << "Failed to load io adaptors " << adaptor
                       << ", reason = " << dlerror();
        } else {
          LOG(INFO) << "Loaded io adaptors " << adaptor;
        }
      }
    }
  } else {
    LOG(INFO) << "No extra loaders provided";
  }
}

void WalWriterFactory::Finalize() {}

std::unique_ptr<IWalWriter> WalWriterFactory::CreateWalWriter(
    const std::string& wal_writer_type) {
  auto& known_writers_ = getKnownWalWriters();
  auto iter = known_writers_.find(wal_writer_type);
  if (iter != known_writers_.end()) {
    return iter->second();
  } else {
    LOG(FATAL) << "Unknown wal writer: " << wal_writer_type;
  }
}

std::unique_ptr<IWalParser> WalWriterFactory::CreateWalParser(
    const std::string& wal_writer_type, const std::string& wal_dir) {
  auto& know_parsers_ = getKnownWalParsers();
  auto iter = know_parsers_.find(wal_writer_type);
  if (iter != know_parsers_.end()) {
    return iter->second(wal_dir);
  } else {
    LOG(ERROR) << "Unknown wal parser: " << wal_writer_type;
    iter = know_parsers_.find("local");  // TODO: remove this
    if (iter != know_parsers_.end()) {
      return iter->second(wal_dir);
    } else {
      LOG(FATAL) << "Unknown wal parser: local";
    }
  }
}

bool WalWriterFactory::RegisterWalWriter(
    const std::string& wal_writer_type,
    WalWriterFactory::wal_writer_initializer_t initializer) {
  LOG(INFO) << "Registering wal writer of type: " << wal_writer_type;
  auto& known_writers_ = getKnownWalWriters();
  known_writers_.emplace(wal_writer_type, initializer);
  return true;
}

bool WalWriterFactory::RegisterWalParser(
    const std::string& wal_writer_type,
    WalWriterFactory::wal_parser_initializer_t initializer) {
  LOG(INFO) << "Registering wal writer of type: " << wal_writer_type;
  auto& known_parsers_ = getKnownWalParsers();
  known_parsers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalWriterFactory::wal_writer_initializer_t>&
WalWriterFactory::getKnownWalWriters() {
  static std::unordered_map<
      std::string, WalWriterFactory::wal_writer_initializer_t>* known_writers_ =
      new std::unordered_map<std::string, wal_writer_initializer_t>();
  return *known_writers_;
}

std::unordered_map<std::string, WalWriterFactory::wal_parser_initializer_t>&
WalWriterFactory::getKnownWalParsers() {
  static std::unordered_map<
      std::string, WalWriterFactory::wal_parser_initializer_t>* known_parsers_ =
      new std::unordered_map<std::string, wal_parser_initializer_t>();
  return *known_parsers_;
}

}  // namespace gs
