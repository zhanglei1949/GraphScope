#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

namespace gs {
// One hop query
class Query2 : public AppBase {
 public:
  Query2(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("medium")),
        center_label_id_(graph.schema().get_vertex_label_id("center")),
        connect_label_id_(graph.schema().get_edge_label_id("connect")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) {
    auto txn = graph_.GetReadTransaction();
    // get center with center id.
    int medium_ids_num = input.get_int();
    std::vector<gs::vid_t> medium_vids(medium_ids_num, 0);
    std::vector<std::string_view> medium_oids(medium_ids_num);
    for (auto i = 0; i < medium_ids_num; ++i) {
      auto medium_id = input.get_string();
      medium_oids[i] = medium_id;
      if (!txn.GetVertexIndex(medium_label_id_, medium_id, medium_vids[i])) {
        txn.Abort();
        LOG(INFO) << medium_id << " medium id not founded\n";
        return false;
      }
    }
    // get center_vid
    gs::vid_t center_vid;
    auto center_oid = input.get_string();
    if (!txn.GetVertexIndex(center_label_id_, center_oid, center_vid)) {
      txn.Abort();
      LOG(INFO) << center_oid << "  not founded\n";
      return false;
    }

    // get all center_ids connected to medium
    std::vector<std::tuple<std::string_view, double, std::string_view>> res_vec;
    for (auto i = 0; i < medium_vids.size(); ++i) {
      auto medium_vid = medium_vids[i];
      auto edge_iter = txn.GetIncomingEdges<double>(
          medium_label_id_, medium_vid, center_label_id_, connect_label_id_);
      for (auto& edge : edge_iter) {
        auto cur_vid = edge.neighbor;
        if (cur_vid != center_vid) {
          auto cur_oid = txn.GetVertexId(center_label_id_, cur_vid);
          res_vec.emplace_back(medium_oids[i], edge.data,
                               cur_oid.AsStringView());
        }
      }
    }
    txn.Abort();
    // write to output
    output.put_int(res_vec.size());
    // LOG(INFO) << "Got res of size: " << res_vec.size();
    for (auto& res : res_vec) {
      output.put_string_view(std::get<0>(res));
      output.put_double(std::get<1>(res));
      output.put_string_view(std::get<2>(res));
    }
    // LOG(INFO) << "res count: " << res_vec.size() << " " << sum << "\n";
    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t medium_label_id_;
  label_t center_label_id_;
  label_t connect_label_id_;
};
}  // namespace gs
extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query2* app = new gs::Query2(db);
  return static_cast<void*>(app);
}
void DeleteApp(void* app) {
  gs::Query2* casted = static_cast<gs::Query2*>(app);
  delete casted;
}
}