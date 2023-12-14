#include <bitset>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/proto_generated_gie/results.pb.h"

namespace gs {
class Query1 : public AppBase {
  static constexpr int limit = 2000;

 public:
  Query1(GraphDBSession& graph)
      : center_label_id_(graph.schema().get_vertex_label_id("center")),
        medium_label_id_(graph.schema().get_vertex_label_id("medium")),
        connect_label_id_(graph.schema().get_edge_label_id("connect")),
        type_name_col_(*(std::dynamic_pointer_cast<StringMapColumn<uint8_t>>(
            graph.get_vertex_property_column(medium_label_id_, "type")))),
        graph_(graph) {
    // duration = 0;
  }
  ~Query1() {
    // LOG(INFO) << "duration = " << duration;
  }
  // Input is (vec<medium_type: string>, center_id: string)
  bool Query(Decoder& input, Encoder& output) {
    // duration -= grape::GetCurrentTime();
    auto txn = graph_.GetReadTransaction();
    int32_t medium_types_num = input.get_int();
    const auto& type_map = type_name_col_.get_meta_map();
    std::bitset<256> valid_types;
    for (auto i = 0; i < medium_types_num; ++i) {
      uint8_t lid;
      auto sw = input.get_string();
      if (!type_map.get_index(sw, lid)) {
        LOG(INFO) << "type " << sw << " not found\n";
      }
      valid_types.set(lid);
    }
    auto center_id = input.get_string();
    CHECK(input.empty());
    gs::vid_t center_vid;
    if (!txn.GetVertexIndex(center_label_id_, center_id, center_vid)) {
      LOG(INFO) << center_id << " not found\n";
      txn.Abort();
      // duration += grape::GetCurrentTime();
      return false;
    }
    // get all medium with center id.
    std::vector<gs::vid_t> medium_vids;
    const auto& edges = txn.GetOutgoingEdges<double>(
        center_label_id_, center_vid, medium_label_id_, connect_label_id_);
    const auto& type_index_col = type_name_col_.get_index_col();
    for (auto& e : edges) {
      auto medium_vid = e.neighbor;

      auto medium_type = type_index_col.get_view(medium_vid);
      if (valid_types.test(medium_type)) {
        medium_vids.push_back(medium_vid);
      }
    }
    //  reverse expand, need the results along each path.
    auto reserver_edge_view = txn.GetIncomingGraphView<double>(
        medium_label_id_, center_label_id_, connect_label_id_);
    int num = 0;
    // size_t begin_loc = output.skip_int();
    results::CollectiveResults res_pb;
    for (auto medium_vid : medium_vids) {
      auto oid = txn.GetVertexId(medium_label_id_, medium_vid).AsStringView();
      const auto& edges = reserver_edge_view.get_edges(medium_vid);
      for (auto& e : edges) {
        auto nbr_vid = e.neighbor;
        if (nbr_vid != center_vid) {
          auto nbr_oid =
              txn.GetVertexId(center_label_id_, nbr_vid).AsStringView();
          auto* cur_row = res_pb.add_results();
          auto record = cur_row->mutable_record();
          // centerId
          auto col1 = record->add_columns();
          col1->mutable_name_or_id()->set_id(0);
          auto obj1 =
              col1->mutable_entry()->mutable_element()->mutable_object();
          obj1->set_str(oid.data(), oid.size());
          // mediumWeight
          auto col2 = record->add_columns();
          col2->mutable_name_or_id()->set_id(1);
          auto obj2 =
              col2->mutable_entry()->mutable_element()->mutable_object();
          obj2->set_f64(e.data);
          // mediumId
          auto col3 = record->add_columns();
          col3->mutable_name_or_id()->set_id(2);
          auto obj3 =
              col3->mutable_entry()->mutable_element()->mutable_object();
          obj3->set_str(nbr_oid.data(), nbr_oid.size());
          // output.put_string_view(oid);
          // output.put_double(e.data);
          // output.put_string_view(nbr_oid);
          ++num;
          if (num == limit) {
            break;
          }
        }
      }
      if (num == limit) {
        break;
      }
    }
    std::string res_str = res_pb.SerializeAsString();
    // encode results to encoder
    output.put_string(res_str);
    // output.put_int_at(begin_loc, num);
    // duration += grape::GetCurrentTime();
    return true;
  }

 private:
  label_t center_label_id_;
  label_t medium_label_id_;
  label_t connect_label_id_;
  const StringMapColumn<uint8_t>& type_name_col_;
  GraphDBSession& graph_;
  // double duration;
};
}  // namespace gs
extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query1* app = new gs::Query1(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query1* casted = static_cast<gs::Query1*>(app);
  delete casted;
}
}