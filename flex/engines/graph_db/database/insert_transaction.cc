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

#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal/wal.h"
#include "flex/engines/graph_db/runtime/utils/cypher_runner_impl.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/allocators.h"
namespace gs {

InsertTransaction::InsertTransaction(const GraphDBSession& session,
                                     MutablePropertyFragment& graph,
                                     Allocator& alloc, IWalWriter& logger,
                                     VersionManager& vm, timestamp_t timestamp)

    : session_(session),
      graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

InsertTransaction::~InsertTransaction() { Abort(); }

std::string InsertTransaction::run(
    const std::string& cypher,
    const std::map<std::string, std::string>& params) {
  return gs::runtime::CypherRunnerImpl::get().run(*this, cypher, params);
}

bool InsertTransaction::AddVertex(label_t label, const Any& id,
                                  const std::vector<Any>& props) {
  size_t arc_size = arc_.GetSize();
  arc_ << static_cast<uint8_t>(0) << label;
  serialize_field(arc_, id);
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    arc_.Resize(arc_size);
    std::string label_name = graph_.schema().get_vertex_label_name(label);
    LOG(ERROR) << "Vertex [" << label_name
               << "] properties size not match, expected " << types.size()
               << ", but got " << props.size();
    return false;
  }
  int col_num = props.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    auto& prop = props[col_i];
    if (prop.type != types[col_i]) {
      if (prop.type == PropertyType::kStringView &&
          types[col_i] == PropertyType::kStringMap) {
      } else {
        arc_.Resize(arc_size);
        std::string label_name = graph_.schema().get_vertex_label_name(label);
        LOG(ERROR) << "Vertex [" << label_name << "][" << col_i
                   << "] property type not match, expected " << types[col_i]
                   << ", but got " << prop.type;
        return false;
      }
    }
    serialize_field(arc_, prop);
  }
  added_vertices_.emplace(label, id);
  return true;
}

bool InsertTransaction::AddEdge(label_t src_label, const Any& src,
                                label_t dst_label, const Any& dst,
                                label_t edge_label, const Any& prop) {
  vid_t lid;
  if (!graph_.get_lid(src_label, src, lid)) {
    if (added_vertices_.find(std::make_pair(src_label, src)) ==
        added_vertices_.end()) {
      std::string label_name = graph_.schema().get_vertex_label_name(src_label);
      VLOG(1) << "Source vertex " << label_name << "[" << src.to_string()
              << "] not found...";
      return false;
    }
  }
  if (!graph_.get_lid(dst_label, dst, lid)) {
    if (added_vertices_.find(std::make_pair(dst_label, dst)) ==
        added_vertices_.end()) {
      std::string label_name = graph_.schema().get_vertex_label_name(dst_label);
      VLOG(1) << "Destination vertex " << label_name << "[" << dst.to_string()
              << "] not found...";
      return false;
    }
  }
  if (prop.type != PropertyType::kRecord) {
    const PropertyType& type =
        graph_.schema().get_edge_property(src_label, dst_label, edge_label);
    if (prop.type != type) {
      std::string label_name = graph_.schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " type not match, expected " << type << ", got "
                 << prop.type;
      return false;
    }
  } else {
    const auto& types =
        graph_.schema().get_edge_properties(src_label, dst_label, edge_label);
    if (prop.AsRecord().size() != types.size()) {
      std::string label_name = graph_.schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " size not match, expected " << types.size() << ", got "
                 << prop.AsRecord().size();
      return false;
    }
    auto r = prop.AsRecord();
    for (size_t i = 0; i < r.size(); ++i) {
      if (r[i].type != types[i]) {
        std::string label_name =
            graph_.schema().get_edge_label_name(edge_label);
        LOG(ERROR) << "Edge property " << label_name
                   << " type not match, expected " << types[i] << ", got "
                   << r[i].type;
        return false;
      }
    }
  }
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
  serialize_field(arc_, prop);
  return true;
}

bool InsertTransaction::Commit() {
  if (timestamp_ == std::numeric_limits<timestamp_t>::max()) {
    return true;
  }
  if (arc_.GetSize() == sizeof(WalHeader)) {
    vm_.release_insert_timestamp(timestamp_);
    clear();
    return true;
  }
  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 0;
  header->timestamp = timestamp_;

  if (!logger_.append(arc_.GetBuffer(), arc_.GetSize())) {
    LOG(ERROR) << "Failed to append wal log";
    Abort();
    return false;
  }
  IngestWal(graph_, timestamp_, arc_.GetBuffer() + sizeof(WalHeader),
            header->length, alloc_);

  vm_.release_insert_timestamp(timestamp_);
  clear();
  return true;
}

void InsertTransaction::Abort() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "aborting " << timestamp_ << "-th transaction (insert)";
    vm_.release_insert_timestamp(timestamp_);
    clear();
  }
}

timestamp_t InsertTransaction::timestamp() const { return timestamp_; }

void InsertTransaction::IngestWal(MutablePropertyFragment& graph,
                                  uint32_t timestamp, char* data, size_t length,
                                  Allocator& alloc) {
  grape::OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    uint8_t op_type;
    arc >> op_type;
    if (op_type == 0) {
      label_t label;
      Any id;
      label = deserialize_oid(graph, arc, id);
      vid_t lid = graph.add_vertex(label, id);
      graph.get_vertex_table(label).ingest(lid, arc);
    } else if (op_type == 1) {
      label_t src_label, dst_label, edge_label;
      Any src, dst;
      vid_t src_lid, dst_lid;
      src_label = deserialize_oid(graph, arc, src);
      dst_label = deserialize_oid(graph, arc, dst);
      arc >> edge_label;

      CHECK(get_vertex_with_retries(graph, src_label, src, src_lid));
      CHECK(get_vertex_with_retries(graph, dst_label, dst, dst_lid));

      graph.IngestEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                       timestamp, arc, alloc);
    } else {
      LOG(FATAL) << "Unexpected op-" << static_cast<int>(op_type);
    }
  }
}

void InsertTransaction::clear() {
  arc_.Clear();
  arc_.Resize(sizeof(WalHeader));
  added_vertices_.clear();

  timestamp_ = std::numeric_limits<timestamp_t>::max();
}

const Schema& InsertTransaction::schema() const { return graph_.schema(); }

const GraphDBSession& InsertTransaction::GetSession() const { return session_; }

#define likely(x) __builtin_expect(!!(x), 1)

bool InsertTransaction::get_vertex_with_retries(MutablePropertyFragment& graph,
                                                label_t label, const Any& oid,
                                                vid_t& lid) {
  if (likely(graph.get_lid(label, oid, lid))) {
    return true;
  }
  for (int i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(1000000));
    if (likely(graph.get_lid(label, oid, lid))) {
      return true;
    }
  }

  LOG(ERROR) << "get_vertex [" << oid.to_string() << "] failed";
  return false;
}

#undef likely

}  // namespace gs
