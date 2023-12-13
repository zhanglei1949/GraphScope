#include <chrono>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/wal.h"

namespace gs {
// Update vertex and edges.
class Query0 : public AppBase {
 public:
  Query0(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("medium")),
        center_label_id_(graph.schema().get_vertex_label_id("center")),
        connect_label_id_(graph.schema().get_edge_label_id("connect")),
        graph_(graph) {
    insert_edges_.reserve(4096);
    insert_vertices_.reserve(4096);
    read_time_ = 0;
    write_time_ = 0;
  }

  ~Query0() {}

  void clear() {
    insert_vertices_.clear();
    insert_edges_.clear();
  }

  bool Query(Decoder& input, Encoder& output) {
    auto begin = std::chrono::system_clock::now();
    vertex_label_num_ = graph_.schema().vertex_label_num();
    edge_label_num_ = graph_.schema().edge_label_num();
    size_t count = static_cast<size_t>(input.get_long());
    UpdateBatch updates;
    for (size_t idx = 0; idx < count; ++idx) {
      const auto& medium_id = input.get_string();
      std::string_view medium_type = input.get_string();
      double medium_weight = input.get_double();
      const auto& center_id = input.get_string();
      double connect_weight = input.get_double();
      updates.AddVertex(center_label_id_, center_id, std::vector<Any>());
      updates.AddVertex(
          medium_label_id_, medium_id,
          std::vector<Any>{Any::From(medium_type), Any::From(medium_weight)});
      updates.AddEdge(center_label_id_, center_id, medium_label_id_, medium_id,
                      connect_label_id_, connect_weight);
    }

    graph_.BatchUpdate(updates);
    clear();
    return true;
  }

 private:
  size_t read_time_;
  size_t write_time_;
  GraphDBSession& graph_;
  label_t medium_label_id_;
  label_t center_label_id_;
  label_t connect_label_id_;

  size_t vertex_label_num_;
  size_t edge_label_num_;
  std::vector<std::tuple<label_t, Any, std::vector<Any>>> insert_vertices_;
  std::vector<std::tuple<label_t, Any, label_t, Any, label_t, Any>>
      insert_edges_;
};

}  // namespace gs
extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query0* app = new gs::Query0(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}