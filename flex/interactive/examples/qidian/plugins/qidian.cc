#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "grape/util.h"

#define RAPIDJSON_HAS_STDSTRING 1

#include <rapidjson/document.h>
#include <rapidjson/pointer.h>
#include <rapidjson/rapidjson.h>
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include <sstream>

#define SPECIALIZE_FOR_TWO_CMP_QUERY
static constexpr const uint32_t Dir_Out = 0x80000000;
static constexpr const uint32_t Dir_In = 0;

namespace gs {

struct Path {
  std::vector<uint32_t> vids;
  std::vector<RecordView> edge_data;
};

struct CustomHash {
  size_t operator()(const std::pair<uint32_t, uint32_t>& key) const {
    uint64_t k = key.first;
    k = (k << 32) | key.second;
    return std::hash<uint64_t>{}(k);
  }
};

struct Results {
  std::unordered_map<std::pair<uint32_t, uint32_t>, std::vector<Path>,
                     CustomHash>
      path_to_end_node;

  void clear() { path_to_end_node.clear(); }
};

/**
 * The layout of vid is as follows
 * 31: direction
 * 30: label
 * 29 ~ 0: vid
 */

inline label_t decode_label(vid_t encoded_vid) {
  return (encoded_vid >> 30) & 0x00000001;
}

inline vid_t decode_vid(vid_t encoded_vid) { return encoded_vid & 0x3FFFFFFF; }

inline vid_t decodev_vid_and_label(vid_t encoded_vid) {
  return 0x7FFFFFFF & encoded_vid;
}
inline uint32_t get_edge_direction(vid_t vid) {
  if (vid & Dir_Out) {
    return Dir_Out;
  } else
    return Dir_In;
}

inline vid_t encode_vid(label_t v_label, const uint32_t& dir, vid_t vid) {
  // vid_t is uint32_t, use the first 1 bits to store label id
  auto res = (((vid_t) v_label << 30) | dir) | vid;
  CHECK(decode_label(res) == v_label);
  CHECK(decode_vid(res) == vid);
  CHECK(get_edge_direction(res) == dir);
  return res;
}

inline uint32_t get_opposite_direction(vid_t vid) {
  if (vid & Dir_Out) {
    return Dir_In;
  } else {
    return Dir_Out;
  }
}

std::string print_debug(std::vector<vid_t>& path) {
  std::stringstream ss;
  for (auto& vid : path) {
    ss << "V[label=" << std::to_string(decode_label(vid))
       << ",dir=" << std::to_string(get_edge_direction(vid))
       << ",vid=" << decode_vid(vid) << "], ";
  }
  return ss.str();
}

std::string rel_type_to_string(int64_t rel_type_id) {
  if (rel_type_id == 0) {
    return "invest";
  } else if (rel_type_id == 1) {
    return "shareholder";
  } else if (rel_type_id == 2) {
    return "shareholder_his";
  } else if (rel_type_id == 3) {
    return "legalperson";
  } else if (rel_type_id == 4) {
    return "legalperson_his";
  } else if (rel_type_id == 5) {
    return "executive";
  } else if (rel_type_id == 6) {
    return "executive_his";
  } else if (rel_type_id == 7) {
    return "branch";
  } else {
    return "unknown";
  }
}

class ResultCreator {
 public:
  ResultCreator(const ReadTransaction& txn)
      : txn_(txn), page_id_(0), page_size_(0) {}

  void Init(
      int32_t page_id, int32_t page_size, label_t person_label_id,
      label_t cmp_label_id,
      std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col,
      std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col) {
    page_id_ = page_id;
    page_size_ = page_size;
    person_label_id_ = person_label_id;
    company_label_id_ = cmp_label_id;
    typed_comp_named_col_ = typed_comp_named_col;
    typed_person_named_col_ = typed_person_named_col;
  }

  bool AddPath(const std::vector<vid_t>& cur_path,
               const std::vector<RecordView>& edge_data) {
    if (cur_path.size() < 2) {
      LOG(ERROR) << "Path size is less than 2";
      return false;
    }
    auto start_node_id = cur_path[0];
    auto end_node_id = cur_path.back();
    auto key = std::make_pair(decodev_vid_and_label(start_node_id),
                              decodev_vid_and_label(end_node_id));
    auto iter = results_.path_to_end_node.find(key);
    if (iter == results_.path_to_end_node.end()) {
      results_.path_to_end_node[key] = std::vector<Path>();
    }
    Path path;
    path.vids = cur_path;
    path.edge_data = edge_data;
    results_.path_to_end_node[key].push_back(path);
    return true;
  }

  std::string Dump() const {
    rapidjson::Document document_(rapidjson::kObjectType);
    document_.AddMember("currentPage", page_id_ + 1, document_.GetAllocator());
    document_.AddMember("pageSize", page_size_, document_.GetAllocator());
    document_.AddMember("data", rapidjson::kArrayType,
                        document_.GetAllocator());
    for (auto& [key, path_list] : results_.path_to_end_node) {
      auto& src_vid = key.first;
      auto& dst_vid = key.second;
      rapidjson::Document paths_for_pair(rapidjson::kObjectType,
                                         &document_.GetAllocator());
      {
        paths_for_pair.AddMember("startNodeName", get_vertex_name(src_vid),
                                 document_.GetAllocator());
        paths_for_pair.AddMember("endNodeName", get_vertex_name(dst_vid),
                                 document_.GetAllocator());
        paths_for_pair.AddMember("paths", rapidjson::kArrayType,
                                 document_.GetAllocator());
        for (auto& path : path_list) {
          paths_for_pair["paths"].PushBack(
              to_json(path, document_.GetAllocator()),
              document_.GetAllocator());
        }
      }

      document_["data"].PushBack(paths_for_pair, document_.GetAllocator());
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document_.Accept(writer);
    return buffer.GetString();
  }

  rapidjson::Document to_json(
      const Path& path, rapidjson::Document::AllocatorType& allocator) const {
    rapidjson::Document path_json(rapidjson::kObjectType, &allocator);
    path_json.AddMember("nodes", rapidjson::kArrayType, allocator);
    path_json.AddMember("relationships", rapidjson::kArrayType, allocator);
    for (auto i = 0; i < path.vids.size(); ++i) {
      rapidjson::Document node(rapidjson::kObjectType, &allocator);
      node.AddMember("id", get_vertex_id(path.vids[i]), allocator);
      node.AddMember("name", get_vertex_name(path.vids[i]), allocator);
      node.AddMember("label", get_vertex_label_name(path.vids[i]),
                     allocator);  // TODO: fix it to company or person
      path_json["nodes"].PushBack(node, allocator);
      if (i < path.vids.size() - 1) {
        auto rel_type = get_rel_type_name(path.edge_data[i][1].AsInt64());
        auto rel_weight = path.edge_data[i][0].AsDouble();
        auto rel_info = path.edge_data[i][2].AsStringView();
        auto rel_detail = path.edge_data[i][3].AsStringView();

        auto relation_id = generate_relation_id(
            path.vids[i], path.vids[i + 1], rel_type,
            get_edge_direction(path.vids[i + 1]));  // path.directions[i]
        rapidjson::Document rel(rapidjson::kObjectType, &allocator);
        auto node_name1 = get_vertex_name(path.vids[i]);
        auto node_name2 = get_vertex_name(path.vids[i + 1]);
        if (path.vids[i + 1] & Dir_Out) {
          rel.AddMember("startNode", node_name1, allocator);
          rel.AddMember("endNode", node_name2, allocator);
        } else {
          rel.AddMember("startNode", node_name2, allocator);
          rel.AddMember("endNode", node_name1, allocator);
        }
        rel.AddMember("type", rel_type, allocator);
        rel.AddMember("name", rel_type, allocator);
        rel.AddMember("id", relation_id, allocator);
        rel.AddMember("properties", rapidjson::kObjectType, allocator);
        rel["properties"].AddMember("weight", rel_weight, allocator);
        rel["properties"].AddMember("label", rel_type, allocator);
        rel["properties"].AddMember("rel_detail", std::string(rel_detail),
                                    allocator);
        rel["properties"].AddMember("rel_info", std::string(rel_info),
                                    allocator);
        rel["properties"].AddMember("id", relation_id, allocator);
        path_json["relationships"].PushBack(rel, allocator);
      }
    }
    return path_json;
  }

 private:
  inline std::string get_vertex_label_name(vid_t vid) const {
    if (decode_label(vid) == person_label_id_) {
      return "person";
    } else
      return "company";
  }

  inline std::string get_vertex_name(vid_t vid) const {
    if (decode_label(vid) == person_label_id_) {
      return std::string(typed_person_named_col_->get_view(decode_vid(vid)));
    } else {
      return std::string(typed_comp_named_col_->get_view(decode_vid(vid)));
    }
  }

  inline int64_t get_vertex_id(vid_t vid) const {
    return txn_.GetVertexId(decode_label(vid), decode_vid(vid)).AsInt64();
  }

  inline std::string get_rel_type_name(int64_t rel_type) const {
    return rel_type_to_string(rel_type);
  }

  inline std::string generate_relation_id(vid_t src, vid_t dst,
                                          const std::string& rel_type,
                                          uint32_t dir) const {
    if (dir == Dir_Out) {
      return std::to_string(get_vertex_id(src)) + "_" + rel_type + "_" +
             std::to_string(get_vertex_id(dst));
    } else {
      return std::to_string(get_vertex_id(dst)) + "_" + rel_type + "_" +
             std::to_string(get_vertex_id(src));
    }
  }

  const ReadTransaction& txn_;
  int32_t page_id_, page_size_;
  label_t person_label_id_, company_label_id_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col_;

  Results results_;
};

class QiDian : public WriteAppBase {
 public:
  static constexpr double timeout_sec = 6;
  static constexpr int32_t REL_TYPE_MAX = 19;  // 0 ~ 18
  QiDian() {}

  void Init(GraphDBSession& graph) {
    company_label_id_ = graph.schema().get_vertex_label_id("company");
    person_label_id_ = graph.schema().get_vertex_label_id("person");
    invest_label_id_ = graph.schema().get_edge_label_id("invest");
    size_t comp_num = graph.graph().vertex_num(company_label_id_);
    size_t person_num = graph.graph().vertex_num(person_label_id_);

    LOG(INFO) << "company num:" << comp_num << ", person num:" << person_num;
    LOG(INFO) << "person_label_id " << static_cast<int>(person_label_id_)
              << ", cmp " << static_cast<int>(company_label_id_);
    valid_comp_vids_.resize(comp_num, false);
    valid_comp_nbrs_.resize(2);
    valid_comp_nbrs_[company_label_id_].resize(comp_num, false);
    valid_comp_nbrs_[person_label_id_].resize(person_num, false);

    auto comp_name_col =
        graph.get_vertex_property_column(company_label_id_, "vertex_name");
    auto person_name_col =
        graph.get_vertex_property_column(person_label_id_, "vertex_name");
    if (!comp_name_col) {
      LOG(ERROR) << "column vertex_name not found for company";
    }
    if (!person_name_col) {
      LOG(ERROR) << "column vertex_name not found for company";
    }
    typed_comp_named_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(comp_name_col);
    typed_person_named_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(
            person_name_col);
    if (!typed_comp_named_col_) {
      LOG(ERROR) << "column vertex_name is not string type for company";
    }
    if (!typed_person_named_col_) {
      LOG(ERROR) << "column vertex_name is not string type for company";
    }

    vis_.resize(2);
    vis_[company_label_id_].resize(comp_num, false);
    vis_[person_label_id_].resize(person_num, false);
  }
  ~QiDian() {}
  inline bool is_simple(vid_t tail) {
    return !vis_[decode_label(tail)][decode_vid(tail)];
  }

  // rel_weigth, rel_type,rel_info,rel_detail
  bool edge_expand(gs::ReadTransaction& txn, const std::vector<vid_t>& vid_vec,
                   std::vector<AdjListView<RecordView>>& edges_list,
                   const std::vector<label_t>& nbr_labels,
                   const std::vector<bool>& valid_rel_type_ids, int32_t cur_ind,
                   std::vector<std::vector<vid_t>>& cur_paths,
                   std::vector<std::vector<RecordView>>& cur_edges_datas,
                   std::vector<std::vector<vid_t>>& next_paths,
                   std::vector<std::vector<RecordView>>& next_edges_datas,
                   size_t& result_size, int32_t left_bound, int32_t right_bound,
                   Encoder& output, ResultCreator& result_creator_,
                   const std::vector<uint32_t>& directions,
                   int32_t& simple_path_cnt, int32_t cur_hop_id,
                   int32_t hop_limit) {  // 1 -> n
    auto& cur_path = cur_paths[cur_ind];
    auto& cur_edges_data = cur_edges_datas[cur_ind];

    double cur_time_left = timeout_sec;

    for (size_t i = 0; i < edges_list.size(); ++i) {
      auto& edges = edges_list[i];
      auto nbr_label = nbr_labels[i];
      for (auto& edge : edges) {
        auto rel_type = edge.get_data()[1].AsInt64();
        if (rel_type >= REL_TYPE_MAX || !valid_rel_type_ids[rel_type]) {
          continue;
        }
        auto raw_dst = edge.get_neighbor();
        if (cur_hop_id == 1) {
          valid_comp_nbrs_[nbr_label][raw_dst] = true;
        }
        auto dst = encode_vid(nbr_label, directions[i], raw_dst);
        if (is_simple(dst)) {
          simple_path_cnt += 1;
          cur_path.emplace_back(dst);
          cur_edges_data.emplace_back(edge.get_data());

          if (cur_hop_id > 1 && cur_hop_id == hop_limit - 1) {
            // When we are now at the second last hop, we could determine
            // whether we are the nbrs of the valid companies
            if (valid_comp_nbrs_[nbr_label][raw_dst]) {
              next_paths.emplace_back(cur_path);
              next_edges_datas.emplace_back(cur_edges_data);
            }
          } else {
            next_paths.emplace_back(cur_path);
            next_edges_datas.emplace_back(cur_edges_data);
          }

          // next_paths.emplace_back(cur_path);
          // next_edges_datas.emplace_back(cur_edges_data);
          if (nbr_label == company_label_id_ && valid_comp_vids_[raw_dst] &&
              decode_vid(cur_path.front()) < decode_vid(cur_path.back())) {
            if (result_size >= left_bound) {
              if (!result_creator_.AddPath(cur_path, cur_edges_data)) {
                LOG(ERROR) << "Add path failed";
                return false;
              }
            }
            ++result_size;

            if (result_size >= right_bound) {
              output.put_string(result_creator_.Dump());
              return cleanUp(txn, vid_vec);
            }
          }
          cur_path.pop_back();
          cur_edges_data.pop_back();
        }
      }
    }
    return false;
  }

  bool edge_expand_bfs(gs::ReadTransaction& txn,
                       const std::vector<vid_t>& vid_vec,
                       std::vector<AdjListView<RecordView>>& edges_list,
                       const std::vector<label_t>& nbr_labels,
                       const std::vector<bool>& valid_rel_type_ids,
                       int32_t cur_ind,
                       std::vector<std::vector<vid_t>>& cur_paths,
                       std::vector<std::vector<RecordView>>& cur_edges_datas,
                       std::vector<std::vector<vid_t>>& next_paths,
                       std::vector<std::vector<RecordView>>& next_edges_datas,
                       const std::vector<uint32_t>& directions,
                       int32_t& simple_path_cnt, int32_t cur_hop_id,
                       int32_t hop_limit) {  // 1 -> n
    auto& cur_path = cur_paths[cur_ind];
    auto& cur_edges_data = cur_edges_datas[cur_ind];

    double cur_time_left = timeout_sec;

    for (size_t i = 0; i < edges_list.size(); ++i) {
      auto& edges = edges_list[i];
      auto nbr_label = nbr_labels[i];
      for (auto& edge : edges) {
        auto rel_type = edge.get_data()[1].AsInt64();
        if (rel_type >= REL_TYPE_MAX || !valid_rel_type_ids[rel_type]) {
          continue;
        }
        auto raw_dst = edge.get_neighbor();
        auto dst = encode_vid(nbr_label, directions[i], raw_dst);
        if (is_simple(dst)) {
          simple_path_cnt += 1;
          cur_path.emplace_back(dst);
          cur_edges_data.emplace_back(edge.get_data());

          next_paths.emplace_back(cur_path);
          next_edges_datas.emplace_back(cur_edges_data);

          cur_path.pop_back();
          cur_edges_data.pop_back();
        }
      }
    }
    return false;
  }

#define DEBUG
  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) {
    int32_t result_limit = 1000000;
    if (!initialized_) {
      Init(graph);
      initialized_ = true;
    }

    auto txn = graph.GetReadTransaction();
    int32_t hop_limit = input.get_int();
    int32_t page_id = input.get_int();
    // Expect page_id indexed from 1.
    if (page_id < 1) {
      LOG(ERROR) << "Invalid page id: " << page_id;
      return false;
    }
    page_id -= 1;
    int32_t page_limit = input.get_int();
    int32_t left_bound = page_id * page_limit;
    int32_t right_bound = (page_id + 1) * page_limit;
    // LOG(INFO) << "result limit: " << page_limit << "\n";
    int32_t rel_type_num = input.get_int();
    // valid rel type ids
    std::vector<bool> valid_rel_type_ids(REL_TYPE_MAX, false);
    for (int i = 0; i < rel_type_num; ++i) {
      auto rel_type = input.get_int();
      if (rel_type < 0 || rel_type >= REL_TYPE_MAX) {
        LOG(ERROR) << "Invalid rel type id: " << rel_type;
        return false;
      }
      valid_rel_type_ids[rel_type] = true;
    }

    int32_t vec_size = input.get_int();
    VLOG(10) << "Group Query: hop limit " << hop_limit << ", page limit "
             << page_limit << ", ids size " << vec_size
             << ", range: " << left_bound << ", " << right_bound;
    if (hop_limit < 1) {
      LOG(ERROR) << "Invalid hop limit: " << hop_limit;
      return false;
    }
    std::vector<vid_t> vid_vec;
    int count = 0;

    for (int i = 0; i < vec_size; ++i) {
      auto oid = input.get_long();
      // std::string_view oid = input.get_string();
      vid_t vid;
      if (!txn.GetVertexIndex(company_label_id_, Any::From(oid), vid)) {
        LOG(INFO) << "Get oid: " << oid << ", not found";
        count++;
      } else {
        VLOG(10) << "Oid: " << oid << ", vid:" << vid;
        vid_vec.emplace_back(vid);
      }
    }
    if (count > 0) {
      LOG(INFO) << count << " out of " << vec_size << " vertices are not found";
    }
    if (count == vec_size) {
      output.put_string("");
      return true;
    }
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = true;
    }
    // Get additional parameter: resultLimit, limit the total number of
    // results(The sum of all pages)
    if (!input.empty()) {
      // Expect a int value.
      if (input.size() != 4) {
        LOG(ERROR) << "Expect a int value for resultLimit " << input.size();
      }
      result_limit = input.get_int();
      LOG(INFO) << "got result_limit: " << result_limit;
    } else {
      LOG(INFO) << "no result limit specified";
    }
    left_bound = std::min(left_bound, result_limit);
    right_bound = std::min(right_bound, result_limit);

    auto cmp_invest_cmp_outgoing_view = txn.GetOutgoingGraphView<RecordView>(
        company_label_id_, company_label_id_, invest_label_id_);
    auto cmp_invest_cmp_incoming_view = txn.GetIncomingGraphView<RecordView>(
        company_label_id_, company_label_id_, invest_label_id_);
    auto person_invest_cmp_outgoing_view = txn.GetOutgoingGraphView<RecordView>(
        person_label_id_, company_label_id_, invest_label_id_);
    auto person_invest_cmp_incoming_view = txn.GetIncomingGraphView<RecordView>(
        company_label_id_, person_label_id_, invest_label_id_);

    ResultCreator result_creator_(txn);
    result_creator_.Init(page_id, page_limit, person_label_id_,
                         company_label_id_, typed_person_named_col_,
                         typed_comp_named_col_);
    std::vector<uint32_t> directions;
    std::vector<label_t> labels;
    directions.push_back(Dir_Out);
    directions.push_back(Dir_In);
    directions.push_back(Dir_In);
    directions.push_back(Dir_Out);
    labels.push_back(company_label_id_);
    labels.push_back(company_label_id_);
    labels.push_back(person_label_id_);
    labels.push_back(company_label_id_);
#ifdef SPECIALIZE_FOR_TWO_CMP_QUERY
    if (vid_vec.size() == 2 && hop_limit >= 4) {
      VLOG(10) << "Specialize for two company query";
      return QueryForTwoCmpQuery(
          txn, vid_vec, hop_limit, left_bound, right_bound, output,
          result_creator_, valid_rel_type_ids, cmp_invest_cmp_outgoing_view,
          cmp_invest_cmp_incoming_view, person_invest_cmp_outgoing_view,
          person_invest_cmp_incoming_view, labels, directions);
    } else {
#endif
      std::fill(valid_comp_nbrs_[company_label_id_].begin(),
                valid_comp_nbrs_[company_label_id_].end(), false);
      std::fill(valid_comp_nbrs_[person_label_id_].begin(),
                valid_comp_nbrs_[person_label_id_].end(), false);
      return QueryForGeneralCase(
          txn, vid_vec, hop_limit, left_bound, right_bound, output,
          result_creator_, valid_rel_type_ids, cmp_invest_cmp_outgoing_view,
          cmp_invest_cmp_incoming_view, person_invest_cmp_outgoing_view,
          person_invest_cmp_incoming_view, labels, directions);
#ifdef SPECIALIZE_FOR_TWO_CMP_QUERY
    }
#endif
  }

  bool cleanUp(ReadTransaction& txn, const std::vector<vid_t>& vid_vec) {
    txn.Commit();
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = false;
    }
    return true;
  }

 private:
  std::pair<std::vector<std::vector<vid_t>>,
            std::vector<std::vector<RecordView>>>
  do_bfs(ReadTransaction& txn, vid_t start, const std::vector<vid_t>& vid_vec,
         int32_t hop_limit, const std::vector<bool>& valid_rel_type_ids,
         const GraphView<RecordView>& cmp_invest_cmp_outgoing_view,
         const GraphView<RecordView>& cmp_invest_cmp_incoming_view,
         const GraphView<RecordView>& person_invest_cmp_outgoing_view,
         const GraphView<RecordView>& person_invest_cmp_incoming_view,
         const std::vector<label_t>& labels,
         const std::vector<uint32_t>& directions) {
    std::vector<std::vector<vid_t>> cur_paths, next_paths;
    std::vector<std::vector<RecordView>> cur_edge_datas, next_edge_datas;

    cur_paths.emplace_back(
        std::vector<vid_t>{encode_vid(company_label_id_, Dir_Out, start)});
    cur_edge_datas.emplace_back(std::vector<RecordView>{});

    std::vector<std::vector<vid_t>> result_paths;
    std::vector<std::vector<RecordView>> result_edge_datas;

    for (auto i = 1; i <= hop_limit; ++i) {
      VLOG(10) << "hop: " << i << "cur path number: " << cur_paths.size();
      int32_t cnt = 0;
      for (auto j = 0; j < cur_paths.size(); ++j) {
        std::vector<AdjListView<RecordView>> edges_list;

        auto last_vid_encoded = cur_paths[j].back();
        auto last_vid = decode_vid(last_vid_encoded);
        auto last_vid_label = decode_label(last_vid_encoded);
        if (last_vid_label == company_label_id_) {
          // const auto& oedges =
          // cmp_invest_cmp_outgoing_view.get_edges(last_vid);
          edges_list.push_back(
              cmp_invest_cmp_outgoing_view.get_edges(last_vid));
          edges_list.push_back(
              cmp_invest_cmp_incoming_view.get_edges(last_vid));
          edges_list.push_back(
              person_invest_cmp_incoming_view.get_edges(last_vid));
        } else {
          edges_list.push_back(
              person_invest_cmp_outgoing_view.get_edges(last_vid));
        }
        for (auto& vid : cur_paths[j]) {
          vis_[decode_label(vid)][decode_vid(vid)] = true;
        }
        edge_expand_bfs(txn, vid_vec, edges_list, labels, valid_rel_type_ids, j,
                        cur_paths, cur_edge_datas, next_paths, next_edge_datas,
                        directions, cnt, i, hop_limit);
        for (auto& vid : cur_paths[j]) {
          vis_[decode_label(vid)][decode_vid(vid)] = false;
        }
      }
      cur_paths.swap(next_paths);
      cur_edge_datas.swap(next_edge_datas);
      next_paths.clear();
      next_edge_datas.clear();
      result_paths.insert(result_paths.end(), cur_paths.begin(),
                          cur_paths.end());
      result_edge_datas.insert(result_edge_datas.end(), cur_edge_datas.begin(),
                               cur_edge_datas.end());
    }
    return std::make_pair(result_paths, result_edge_datas);
  }

  // Runs only when hop_limit >= 4.
  // There are different cases.
  // [source]->[dst]: source just connected to dst
  // [source]->[...]->[dst]: source connect to dst with hop < hop_limit.
  // [source]->[...]->[dst]: source connect to dst just with hop_limit.
  bool QueryForTwoCmpQuery(
      ReadTransaction& txn, const std::vector<vid_t>& vid_vec,
      int32_t hop_limit, int32_t left_bound, int32_t right_bound,
      Encoder& output, ResultCreator& result_creator_,
      const std::vector<bool>& valid_rel_type_ids,
      const GraphView<RecordView>& cmp_invest_cmp_outgoing_view,
      const GraphView<RecordView>& cmp_invest_cmp_incoming_view,
      const GraphView<RecordView>& person_invest_cmp_outgoing_view,
      const GraphView<RecordView>& person_invest_cmp_incoming_view,
      const std::vector<label_t>& labels,
      const std::vector<uint32_t>& directions) {
    CHECK(vid_vec.size() == 2);
    size_t result_size = 0;

    int32_t right_hop_limit = hop_limit / 2;
    int32_t left_hop_limit = hop_limit - right_hop_limit;
    std::vector<std::vector<vid_t>> left_result_paths, right_result_paths;
    std::vector<std::vector<RecordView>> left_result_edge_datas,
        right_result_edge_datas;
    // std::thread left_thread = std::thread([&]() {
    std::tie(left_result_paths, left_result_edge_datas) =
        do_bfs(txn, vid_vec[0], vid_vec, left_hop_limit, valid_rel_type_ids,
               cmp_invest_cmp_outgoing_view, cmp_invest_cmp_incoming_view,
               person_invest_cmp_outgoing_view, person_invest_cmp_incoming_view,
               labels, directions);
    // });

    // std::thread right_thread = std::thread([&]() {
    std::tie(right_result_paths, right_result_edge_datas) =
        do_bfs(txn, vid_vec[1], vid_vec, right_hop_limit, valid_rel_type_ids,
               cmp_invest_cmp_outgoing_view, cmp_invest_cmp_incoming_view,
               person_invest_cmp_outgoing_view, person_invest_cmp_incoming_view,
               labels, directions);
    // });
    // left_thread.join();
    // right_thread.join();
    // After bidirectional bfs, we have got all the paths, now we need to merge
    // them
    //  next_paths have already been swaped to cur_paths
    // LOG(INFO) << "right result paths size: " << right_result_paths.size();

    // We always need to merge from start.
    double construct_hash_map_time = -grape::GetCurrentTime();
    std::unordered_map<vid_t, std::vector<int32_t>>
        left_vid_to_ind;  // the last vid to the index in left_cur_paths
    for (auto i = 0; i < left_result_paths.size(); ++i) {
      // If we found a src->dst match in left_cur_paths, just add the path to
      // the result.
      auto raw_vid_label = decodev_vid_and_label(left_result_paths[i].back());
      if (decode_label(raw_vid_label) == company_label_id_ &&
          decode_vid(raw_vid_label) == vid_vec[1]) {
        if (result_size >= left_bound) {
          if (!result_creator_.AddPath(left_result_paths[i],
                                       left_result_edge_datas[i])) {
            LOG(ERROR) << "Add path failed";
            return false;
          }
        }
        result_size++;

        if (result_size >= right_bound) {
          output.put_string(result_creator_.Dump());
          return cleanUp(txn, vid_vec);
        }
      } else {
        if (left_vid_to_ind.find(raw_vid_label) == left_vid_to_ind.end()) {
          left_vid_to_ind[raw_vid_label] = std::vector<int32_t>{i};
        } else {
          left_vid_to_ind[raw_vid_label].emplace_back(i);
        }
      }
    }
    //    LOG(INFO) << "Already got " << result_size << " results from left
    //    paths";
    construct_hash_map_time += grape::GetCurrentTime();
    // VLOG(10) << "construct hash map time: " << construct_hash_map_time
    //         << ", map size: " << left_vid_to_ind.size();
    double merge_path_t = -grape::GetCurrentTime();
    for (auto i = 0; i < right_result_paths.size(); ++i) {
      auto& right_path = right_result_paths[i];
      auto& right_edges_data = right_result_edge_datas[i];
      auto intersect_encoded_vid = right_path.back();
      auto raw_intersect_vid = decodev_vid_and_label(intersect_encoded_vid);
      if (left_vid_to_ind.find(raw_intersect_vid) != left_vid_to_ind.end()) {
        auto& left_inds = left_vid_to_ind[raw_intersect_vid];
        for (auto left_ind : left_inds) {
          auto merged_path = left_result_paths[left_ind];
          for (auto& vid : merged_path) {
            vis_[decode_label(vid)][decode_vid(vid)] = true;
          }
          bool flag = true;
          for (auto j = 0; j < right_path.size() - 1; ++j) {
            auto encoded_vid = right_path[right_path.size() - 2 - j];
            auto& prev_vid = right_path[right_path.size() - 1 - j];
            auto cur_label = decode_label(encoded_vid);
            auto cur_vid = decode_vid(encoded_vid);
            if (vis_[cur_label][cur_vid]) {
              flag = false;
              break;
            }
            auto new_encoded_vid = encode_vid(
                cur_label, get_opposite_direction(prev_vid), cur_vid);
            merged_path.emplace_back(new_encoded_vid);
            vis_[cur_label][cur_vid] = true;
          }

          for (auto& vid : merged_path) {
            vis_[decode_label(vid)][decode_vid(vid)] = false;
          }

          if (flag) {
            if (result_size >= left_bound) {
              auto merged_edges_data = left_result_edge_datas[left_ind];
              for (auto j = 0; j < right_edges_data.size(); ++j) {
                merged_edges_data.emplace_back(
                    right_edges_data[right_edges_data.size() - j - 1]);
              }
              if (!result_creator_.AddPath(merged_path, merged_edges_data)) {
                LOG(ERROR) << "Add path failed";
                return false;
              }
            }
            result_size++;
            if (result_size >= right_bound) {
              merge_path_t += grape::GetCurrentTime();
              // VLOG(10) << "Early termination merge path time: " <<
              // merge_path_t
              //         << ", result size: " << result_size;
              output.put_string(result_creator_.Dump());
              return cleanUp(txn, vid_vec);
            }
          }
        }
      }
    }
    merge_path_t += grape::GetCurrentTime();
    // LOG(INFO) << "merge path time: " << merge_path_t
    //         << ", result size: " << result_size;

    output.put_string(result_creator_.Dump());
    return cleanUp(txn, vid_vec);
  }

  bool QueryForGeneralCase(
      ReadTransaction& txn, const std::vector<vid_t>& vid_vec,
      int32_t hop_limit, int32_t left_bound, int32_t right_bound,
      Encoder& output, ResultCreator& result_creator_,
      const std::vector<bool>& valid_rel_type_ids,
      const GraphView<RecordView>& cmp_invest_cmp_outgoing_view,
      const GraphView<RecordView>& cmp_invest_cmp_incoming_view,
      const GraphView<RecordView>& person_invest_cmp_outgoing_view,
      const GraphView<RecordView>& person_invest_cmp_incoming_view,
      const std::vector<label_t>& labels,
      const std::vector<uint32_t>& directions) {
    // Expand from vid_vec, until end_vertex is valid, or hop limit is reached.
    std::vector<std::vector<vid_t>> cur_paths;
    std::vector<std::vector<RecordView>> cur_edges_datas;
    std::vector<std::vector<vid_t>> next_paths;
    std::vector<std::vector<RecordView>> next_edges_datas;

    // init cur_paths
    for (auto& vid : vid_vec) {
      cur_paths.emplace_back(
          std::vector<vid_t>{encode_vid(company_label_id_, Dir_Out, vid)});
      cur_edges_datas.emplace_back(std::vector<RecordView>{});
    }
    // size_t begin_loc = output.skip_int();
    size_t result_size = 0;
    for (auto i = 1; i <= hop_limit; ++i) {
      VLOG(10) << "hop: " << i << "cur path number: " << cur_paths.size();
      int32_t cnt = 0;
      for (auto j = 0; j < cur_paths.size(); ++j) {
        std::vector<AdjListView<RecordView>> edges_list;

        auto last_vid_encoded = cur_paths[j].back();
        auto last_vid = decode_vid(last_vid_encoded);
        auto last_vid_label = decode_label(last_vid_encoded);
        if (last_vid_label == company_label_id_) {
          // const auto& oedges =
          // cmp_invest_cmp_outgoing_view.get_edges(last_vid);
          edges_list.push_back(
              cmp_invest_cmp_outgoing_view.get_edges(last_vid));
          edges_list.push_back(
              cmp_invest_cmp_incoming_view.get_edges(last_vid));
          edges_list.push_back(
              person_invest_cmp_incoming_view.get_edges(last_vid));
        } else {
          edges_list.push_back(
              person_invest_cmp_outgoing_view.get_edges(last_vid));
        }
        for (auto& vid : cur_paths[j]) {
          vis_[decode_label(vid)][decode_vid(vid)] = true;
        }
        if (edge_expand(txn, vid_vec, edges_list, labels, valid_rel_type_ids, j,
                        cur_paths, cur_edges_datas, next_paths,
                        next_edges_datas, result_size, left_bound, right_bound,
                        output, result_creator_, directions, cnt, i,
                        hop_limit)) {
          return true;  // early terminate.
        }
        for (auto& vid : cur_paths[j]) {
          vis_[decode_label(vid)][decode_vid(vid)] = false;
        }
      }
      cur_paths.swap(next_paths);
      cur_edges_datas.swap(next_edges_datas);
      next_paths.clear();
      next_edges_datas.clear();
    }

    output.put_string(result_creator_.Dump());
    return cleanUp(txn, vid_vec);
  }

  label_t company_label_id_;
  label_t person_label_id_;
  label_t invest_label_id_;
  // std::unordered_set<vid_t> vis_;
  std::vector<std::vector<bool>> vis_;  // for each label, maintain a set
  std::vector<bool> valid_comp_vids_;
  std::vector<std::vector<bool>>
      valid_comp_nbrs_;  // neighbors of valid companies
  bool initialized_ = false;

  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col_;
};

#undef DEBUG

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::QiDian* app = new gs::QiDian();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::QiDian* casted = static_cast<gs::QiDian*>(app);
  delete casted;
}
}
