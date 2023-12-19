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
#ifndef ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_
#define ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_

#include <future>
#include <vector>
#include "flex/storages/rt_mutable_graph/schema.h"
#include "flex/third_party/httplib.h"
#include "flex/utils/app_utils.h"
#include "flex/utils/property/types.h"
#include "flex/utils/result.h"

namespace server {
class GraphDBUpdateServer {
 public:
  static GraphDBUpdateServer& get() {
    static GraphDBUpdateServer db_server;
    return db_server;
  }

  static void init_client(httplib::Client*& cli, const std::string& url,
                          int port) {
    if (cli != nullptr) {
      delete cli;
    }
    cli = new httplib::Client(url, port);
    cli->set_connection_timeout(0, 300000);
    cli->set_read_timeout(60, 0);
    cli->set_write_timeout(60, 0);
  }

  static void init(const std::string& url, int port) {
    auto& server = GraphDBUpdateServer::get();
    server.url_ = url;
    server.port_ = port;
    init_client(server.cli_, url, port);
  }

  ~GraphDBUpdateServer() {
    if (cli_ != nullptr) {
      delete cli_;
    }
  }

  auto parse_prop_map(gs::Decoder& output) {
    int len = output.get_int();
    std::unordered_map<std::string_view, std::string_view> prop_map;
    for (int i = 0; i < len; ++i) {
      const auto& key = output.get_string();
      const auto& value = output.get_string();
      prop_map[key] = value;
    }

    return prop_map;
  }

  template <typename T>
  void get_numerical_val(const std::string_view& str, T& val) {
    val = 0;
    for (char c : str) {
      val = val * 10 + c - '0';
    }
  }
  void parse_prop_value(const std::string_view& prop_str,
                        const gs::PropertyType& type) {
    if (type == gs::PropertyType::kString ||
        type == gs::PropertyType::kStringMap ||
        type.type_enum == gs::impl::PropertyTypeImpl::kVarChar) {
      encoder_.put_string_view(prop_str);
      // return gs::Any::From(prop_str);
    } else if (type == gs::PropertyType::kInt64) {
      int64_t val;
      get_numerical_val(prop_str, val);
      encoder_.put_long(val);
    } else if (type == gs::PropertyType::kInt32) {
      int32_t val;
      get_numerical_val(prop_str, val);
      encoder_.put_int(val);
    } else if (type == gs::PropertyType::kDate) {
      int64_t val;
      get_numerical_val(prop_str, val);
      encoder_.put_long(val);
    } else if (type == gs::PropertyType::kDouble) {
      auto prop = std::string(prop_str.data(), prop_str.size());
      double val = std::atof(prop.c_str());
      encoder_.put_double(val);
    } else if (type == gs::PropertyType::kFloat) {
      auto prop = std::string(prop_str.data(), prop_str.size());
      double val = std::atof(prop.c_str());
      encoder_.put_float(val);
    } else if (type == gs::PropertyType::kBool) {
      bool val = (prop_str == "true" || prop_str == "1");
      encoder_.put_byte(val);
    } else if (type == gs::PropertyType::kUInt32) {
      uint32_t val;
      get_numerical_val(prop_str, val);
      encoder_.put_uint(val);
    } else if (type == gs::PropertyType::kUInt64) {
      uint64_t val;
      get_numerical_val(prop_str, val);
      encoder_.put_ulong(val);
    } else if (type == gs::PropertyType::kEmpty) {
    } else {
      LOG(INFO) << "Unknow type" << (int) (type.type_enum) << "\n";
    }
  }

  void parse_vertex(gs::Decoder& output) {
    const auto& label = output.get_string();

    std::string label_name = std::string(label.data(), label.size());

    auto label_id = schema_.get_vertex_label_id(label_name);
    encoder_.put_byte(label_id);
    const auto& prop_names = schema_.get_vertex_property_names(label_id);
    const auto& prop_types = schema_.get_vertex_properties(label_id);
    const auto& vertex_prop = parse_prop_map(output);
    const auto& prim_keys = schema_.get_vertex_primary_key(label_id);
    for (const auto& [type, name, _] : prim_keys) {
      parse_prop_value(vertex_prop.at(name), type);
    }
    for (size_t i = 0; i < prop_names.size(); ++i) {
      const auto& name = prop_names[i];
      const auto& type = prop_types[i];
      auto prop_name = vertex_prop.at(name);
      parse_prop_value(prop_name, type);
    }
  }

  void parse_edge(gs::Decoder& output) {
    const auto& src_label = output.get_string();
    std::string src_label_str = std::string(src_label.data(), src_label.size());

    const auto& dst_label = output.get_string();

    const auto& edge_label = output.get_string();

    gs::label_t src_label_id = schema_.get_vertex_label_id(src_label_str);
    encoder_.put_byte(src_label_id);
    const auto& src_pk = parse_prop_map(output);
    const auto& src_prim_keys = schema_.get_vertex_primary_key(src_label_id);
    for (const auto& [type, name, _] : src_prim_keys) {
      parse_prop_value(src_pk.at(name), type);
      // break;
    }

    std::string dst_label_str = std::string(dst_label.data(), dst_label.size());
    gs::label_t dst_label_id = schema_.get_vertex_label_id(dst_label_str);
    encoder_.put_byte(dst_label_id);
    const auto& dst_pk = parse_prop_map(output);
    const auto& dst_prim_keys = schema_.get_vertex_primary_key(dst_label_id);
    for (const auto& [type, name, _] : dst_prim_keys) {
      parse_prop_value(dst_pk.at(name), type);
    }

    std::string edge_label_str =
        std::string(edge_label.data(), edge_label.size());

    gs::label_t edge_label_id = schema_.get_edge_label_id(edge_label_str);
    encoder_.put_byte(edge_label_id);
    const auto& prop = parse_prop_map(output);
    const auto& edge_prop =
        schema_.get_edge_properties(src_label_id, dst_label_id, edge_label_id);
    const auto& edge_prop_name = schema_.get_edge_property_names(
        src_label_id, dst_label_id, edge_label_id);
    for (size_t i = 0; i < edge_prop.size(); ++i) {
      const auto& name = edge_prop_name[i];
      const auto& type = edge_prop[i];
      parse_prop_value(prop.at(name), type);
    }
  }
  void parse_query(const std::string& origin_query, std::string& query) {
    gs::Decoder output(origin_query.data(), origin_query.size());
    size_t len = 0;
    data_.clear();
    encoder_.clear();
    int vertex_len = output.get_int();
    encoder_.put_int(vertex_len);
    for (int i = 0; i < vertex_len; ++i) {
      parse_vertex(output);
    }
    int edge_len = output.get_int();
    encoder_.put_int(edge_len);
    for (int i = 0; i < edge_len; ++i) {
      parse_edge(output);
    }
    // LOG(INFO) << " " << data_.size() << "\n";
    encoder_.put_byte(output.get_byte());
    query = std::string(data_.data(), data_.size());
  }

  gs::Result<std::vector<char>> Eval(std::string&& origin_query) {
    std::string query{};
    static constexpr uint8_t flag = (1 << 7);
    bool forward = false;
    if (static_cast<uint8_t>(origin_query.back()) >= flag) {
      origin_query.back() -= flag;
      forward = true;
    }
    if (origin_query.back() == 0) {
      int64_t cur = *static_cast<const int64_t*>(
          static_cast<const void*>(origin_query.data()));
      std::vector<char> data(8);

      Forward();
      if (commit_.valid()) {
        commit_.get();
      }
      int64_t ts = timestamp_;
      memcpy(data.data(), &ts, sizeof(ts));
      return data;
    }
    parse_query(origin_query, query);

    size_t len = query.size();
    if (size_ + sizeof(size_t) + len >= update_queries_.capacity()) {
      Forward();
    }
    int64_t ts = timestamp_;
    ++count_;
    memcpy(update_queries_.data(), &count_, sizeof(int));
    while (sizeof(size_t) + len >= update_queries_.capacity()) {
      update_queries_.reserve(2 * update_queries_.capacity());
    }
    memcpy(update_queries_.data() + size_, &len, sizeof(size_t));
    size_ += sizeof(size_t);
    memcpy(update_queries_.data() + size_, query.data(), len);
    size_ += len;
    if (forward) {
      Forward();
    }
    std::vector<char> data(8);
    memcpy(data.data(), &ts, sizeof(ts));
    return data;
  }

  void Forward() {
    printf("Post %ld\n", size_);
    if (size_ <= 4) {
      if (commit_.valid())
        commit_.get();
      return;
    }
    LOG(INFO) << "count: " << count_ << " size: " << size_ << "\n";
    char type = 3;
    memcpy(update_queries_.data() + size_, &type, sizeof(char));
    size_ += sizeof(char);
    LOG(INFO) << "size: " << size_ << "\n";
    {
      if (commit_.valid()) {
        commit_.get();
      }
      update_queries_.swap(temp_);
      commit_ = std::async(
          std::launch::async,
          [&](size_t size) {
            LOG(INFO) << "size: " << size << " type: " << (int) temp_[size - 1]
                      << " " << int(update_queries_[size - 1]) << "\n";
            const auto& result = cli_->Post("/interactive/update", temp_.data(),
                                            size, "text/plain");
            printf("code: %d\n", result->status);
            if (result->status == 200) {
              return true;
            } else {
              return false;
            }
          },
          size_);
      timestamp_ += 1;

      size_ = 4;
      count_ = 0;
    }
  }

  void init(gs::Schema& schema) { schema_ = schema; }

 private:
  std::vector<char> data_;
  gs::Encoder encoder_;
  std::vector<char> update_queries_;
  std::vector<char> temp_;
  size_t size_;
  int count_;
  std::string url_;
  int port_;
  httplib::Client* cli_;
  gs::Schema schema_;
  gs::timestamp_t timestamp_;
  std::future<bool> commit_;
  GraphDBUpdateServer()
      : size_(4),
        count_(0),
        url_("127.0.0.1"),
        port_(10000),
        cli_(nullptr),
        encoder_(data_),
        timestamp_(1) {
    data_.reserve(4096);
    update_queries_.reserve(4096 * 4096);
    temp_.reserve(4096 * 4096);
    init_client(cli_, url_, port_);
  };
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_GRAPH_DB_UPDATE_SERVER_H_
