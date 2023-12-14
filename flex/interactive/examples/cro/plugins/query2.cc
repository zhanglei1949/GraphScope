#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/proto_generated_gie/results.pb.h"

namespace gs {
// One hop query
class Query2 : public AppBase {
 public:
  Query2(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("medium")),
        center_label_id_(graph.schema().get_vertex_label_id("center")),
        connect_label_id_(graph.schema().get_edge_label_id("connect")),
        graph_(graph) {}

  // input is (vector<medium_id: string>, center_id: string)
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
    // output.put_int(res_vec.size());
    // // LOG(INFO) << "Got res of size: " << res_vec.size();
    // for (auto& res : res_vec) {
    //   output.put_string_view(std::get<0>(res));
    //   output.put_double(std::get<1>(res));
    //   output.put_string_view(std::get<2>(res));
    // }
    results::CollectiveResults res_pb;
    for (auto& path : res_vec) {
      auto* r = res_pb.add_results();
      auto record = r->mutable_record();
      // centerId
      auto col1 = record->add_columns();
      col1->mutable_name_or_id()->set_id(0);
      auto obj1 = col1->mutable_entry()->mutable_element()->mutable_object();
      obj1->set_str(std::get<0>(path).data(), std::get<0>(path).size());
      // mediumWeight
      auto col2 = record->add_columns();
      col2->mutable_name_or_id()->set_id(1);
      auto obj2 = col2->mutable_entry()->mutable_element()->mutable_object();
      obj2->set_f64(std::get<1>(path));
      // mediumId
      auto col3 = record->add_columns();
      col3->mutable_name_or_id()->set_id(2);
      auto obj3 = col3->mutable_entry()->mutable_element()->mutable_object();
      obj3->set_str(std::get<2>(path).data(), std::get<2>(path).size());
    }
    std::string res_str = res_pb.SerializeAsString();
    // encode results to encoder
    output.put_string(res_str);
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