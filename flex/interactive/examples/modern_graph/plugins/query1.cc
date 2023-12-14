#include <bitset>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/proto_generated_gie/results.pb.h"

namespace gs {
class Query1 : public AppBase {
  static constexpr int limit = 2000;

 public:
  Query1(GraphDBSession& graph)
      : graph_(graph) {
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
    for (auto i = 0; i < medium_types_num; ++i) {
      uint8_t lid;
      auto sw = input.get_string();
      LOG(INFO) << sw;
    }
    auto center_id = input.get_string();
    int num = 0;
    // size_t begin_loc = output.skip_int();
    std::vector<std::tuple<std::string, double, std::string>> res_vec;
    res_vec.emplace_back("a", 1.0, "b");
    res_vec.emplace_back("c", 2.0, "d");
    results::CollectiveResults res_pb;
    for (auto& tuple : res_vec) {
          auto* cur_row = res_pb.add_results();
          auto record = cur_row->mutable_record();
          // centerId
          auto col1 = record->add_columns();
          col1->mutable_name_or_id()->set_id(0);
          auto obj1 =
              col1->mutable_entry()->mutable_element()->mutable_object();
          obj1->set_str(std::get<0>(tuple).data(), std::get<0>(tuple).size());
          // mediumWeight
          auto col2 = record->add_columns();
          col2->mutable_name_or_id()->set_id(1);
          auto obj2 =
              col2->mutable_entry()->mutable_element()->mutable_object();
          obj2->set_f64(std::get<1>(tuple));
          // mediumId
          auto col3 = record->add_columns();
          col3->mutable_name_or_id()->set_id(2);
          auto obj3 =
              col3->mutable_entry()->mutable_element()->mutable_object();
          obj3->set_str(std::get<2>(tuple).data(), std::get<2>(tuple).size());
          // output.put_string_view(oid);
          // output.put_double(e.data);
          // output.put_string_view(nbr_oid);
          ++num;
    }
    std::string res_str = res_pb.SerializeAsString();
    // encode results to encoder
    output.put_string(res_str);
    // output.put_int_at(begin_loc, num);
    // duration += grape::GetCurrentTime();
    return true;
  }

 private:
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
