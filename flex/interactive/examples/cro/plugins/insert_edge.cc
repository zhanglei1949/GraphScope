#include <chrono>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/wal.h"

namespace gs {

// This procedure is registered via raw .so, and compiler will not see it.
// Update vertex and edges.
class InsertEdge : public AppBase {
 public:
  InsertEdge(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("medium")),
        center_label_id_(graph.schema().get_vertex_label_id("center")),
        connect_label_id_(graph.schema().get_edge_label_id("connect")),
        graph_(graph) {
    // insert_edges_.reserve(4096);
    // insert_vertices_.reserve(4096);
    // read_time_ = 0;
    // write_time_ = 0;
  }

  ~InsertEdge() {}

  void clear() {
    // insert_vertices_.clear();
    // insert_edges_.clear();
  }

  bool Query(Decoder& input, Encoder& output) {
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
  GraphDBSession& graph_;
  label_t medium_label_id_;
  label_t center_label_id_;
  label_t connect_label_id_;

  size_t vertex_label_num_;
};

}  // namespace gs
extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::InsertEdge* app = new gs::InsertEdge(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::InsertEdge* casted = static_cast<gs::InsertEdge*>(app);
  delete casted;
}
}