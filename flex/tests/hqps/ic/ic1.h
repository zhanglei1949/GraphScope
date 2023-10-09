#ifndef IC1_H
#define IC1_H

#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

namespace gs {
template <typename Out>
void split(const std::string_view& s, char delim, Out result) {
  std::string str(s.data(), s.size());
  std::istringstream iss(str);
  std::string item;
  while (std::getline(iss, item, delim)) {
    *result++ = item;
  }
}

std::vector<std::string> split(const std::string_view& s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, std::back_inserter(elems));
  return elems;
}

// Auto generated expression class definition
struct IC1left_left_left_expr0 {
 public:
  using result_t = bool;
  IC1left_left_left_expr0(int64_t personId) : personId_(personId) {}

  inline auto operator()(int64_t id) const { return id == personId_; }

 private:
  int64_t personId_;
};

struct IC1left_left_left_expr1 {
 public:
  using result_t = bool;
  IC1left_left_left_expr1(std::string_view firstName, int64_t personid)
      : firstName_0_(firstName), personid_(personid) {}

  inline auto operator()(std::string_view firstName, int64_t id) const {
    return firstName == firstName_0_ && id != personid_;
  }

 private:
  std::string_view firstName_0_;
  int64_t personid_;
};

struct IC1left_left_right_expr0 {
 public:
  using result_t = bool;
  IC1left_left_right_expr0() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC1left_left_left_expr2 {
 public:
  using result_t = std::tuple<std::string_view, int32_t, std::string_view>;
  ;
  IC1left_left_left_expr2() {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var7, std::string_view name,
                         int32_t workFrom, std::string_view name_0) const {
    if (IsNull(var7)) {
      return NullRecordCreator<result_t>::GetNull();
    }

    return std::tuple{name, workFrom, name_0};
    ;
  }

 private:
};

struct IC1left_right_expr0 {
 public:
  using result_t = bool;
  IC1left_right_expr0() {}

  inline auto operator()() const { return true; }

 private:
};

struct IC1left_left_left_expr5 {
 public:
  using result_t = std::tuple<std::string_view, int32_t, std::string_view>;

  IC1left_left_left_expr5() {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var11, std::string_view name,
                         int32_t classYear, std::string_view name_0) const {
    if (IsNull(var11)) {
      return NullRecordCreator<result_t>::GetNull();
    }

    return std::tuple{name, classYear, name_0};
    ;
  }

 private:
};

struct IC1right_expr0 {
 public:
  using result_t = bool;
  IC1right_expr0() {}

  inline auto operator()() const { return true; }

 private:
};

// Auto generated query class definition
class IC1 {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  // Query function for query class

 private:
  double path_expand_time = 0.0;
  double scan_time_1 = 0.0;
  double scan_time_2 = 0.0;
  double scan_time_3 = 0.0;
  double scan_time_4 = 0.0;
  double get_v_time = 0.0;
  double get_v_from_path_time = 0.0;
  double proj_time = 0.0;
  double group_time = 0.0;
  double sort_time = 0.0;

 public:
  ~IC1() {
    LOG(INFO) << "path_expand_time: " << path_expand_time;
    LOG(INFO) << "scan_time_1: " << scan_time_1;
    LOG(INFO) << "scan_time_2: " << scan_time_2;
    LOG(INFO) << "scan_time_3: " << scan_time_3;
    LOG(INFO) << "scan_time_4: " << scan_time_4;
    LOG(INFO) << "get_v_time: " << get_v_time;
    LOG(INFO) << "get_v_from_path_time: " << get_v_from_path_time;
    LOG(INFO) << "proj_time: " << proj_time;
    LOG(INFO) << "group_time: " << group_time;
    LOG(INFO) << "sort_time: " << sort_time;
  }

  void Query(const gs::MutableCSRInterface& graph, Decoder& input,
             Encoder& output) {
    int64_t personId = input.get_long();
    std::string_view firstName_0 = input.get_string();

    auto left_left_left_expr0 = gs::make_filter(
        IC1left_left_left_expr0(personId), gs::PropertySelector<int64_t>("id"));
    double t0 = -grape::GetCurrentTime();
    auto left_left_left_ctx0 =
        Engine::template ScanVertex<gs::AppendOpt::Persist>(
            graph, 1, std::move(left_left_left_expr0));
    t0 += grape::GetCurrentTime();
    scan_time_1 += t0;

    auto left_left_left_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto left_left_left_get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{(label_id_t) 1});

    auto left_left_left_path_opt2 = gs::make_path_expandv_opt(
        std::move(left_left_left_edge_expand_opt1),
        std::move(left_left_left_get_v_opt0), gs::Range(1, 4));
    double t1 = -grape::GetCurrentTime();
    auto left_left_left_ctx1 =
        Engine::PathExpandP<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_left_left_ctx0),
            std::move(left_left_left_path_opt2));
    t1 += grape::GetCurrentTime();
    path_expand_time += t1;

    double t2 = -grape::GetCurrentTime();
    auto left_left_left_get_v_opt3 =
        make_getv_opt(gs::VOpt::End, std::array<label_id_t, 0>{});
    auto left_left_left_ctx2 =
        Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(left_left_left_ctx1),
            std::move(left_left_left_get_v_opt3));
    t2 += grape::GetCurrentTime();
    get_v_from_path_time += t2;

    double t22 = -grape::GetCurrentTime();
    auto left_left_left_expr2 =
        gs::make_filter(IC1left_left_left_expr1(firstName_0, personId),
                        gs::PropertySelector<std::string_view>("firstName"),
                        gs::PropertySelector<int64_t>("id"));
    auto left_left_left_get_v_opt4 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{},
                      std::move(left_left_left_expr2));
    auto left_left_left_ctx3 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_left_left_ctx2),
            std::move(left_left_left_get_v_opt4));
    t22 += grape::GetCurrentTime();
    get_v_time += t22;

    double t3 = -grape::GetCurrentTime();
    auto left_left_left_ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<LengthKey>("length"))});
    t3 += grape::GetCurrentTime();
    proj_time += t3;

    GroupKey<0, grape::EmptyType> left_left_left_group_key5(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_left_agg_func6 = gs::make_aggregate_prop<gs::AggFunc::MIN>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    double t4 = -grape::GetCurrentTime();
    auto left_left_left_ctx5 =
        Engine::GroupBy(graph, std::move(left_left_left_ctx4),
                        std::tuple{left_left_left_group_key5},
                        std::tuple{left_left_left_agg_func6});
    t4 += grape::GetCurrentTime();
    group_time += t4;

    double t5 = -grape::GetCurrentTime();
    auto left_left_left_ctx6 = Engine::Sort(
        graph, std::move(left_left_left_ctx5), gs::Range(0, 20),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::ASC, 1, int32_t>(""),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>(
                "lastName"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id")});
    t5 += grape::GetCurrentTime();
    sort_time += t5;

    double t6 = -grape::GetCurrentTime();
    auto left_left_right_expr0 = gs::make_filter(IC1left_left_right_expr0());
    auto left_left_right_ctx0 =
        Engine::template ScanVertex<gs::AppendOpt::Persist>(
            graph, 1, std::move(left_left_right_expr0));
    t6 += grape::GetCurrentTime();
    scan_time_2 += t6;

    auto left_left_right_edge_expand_opt0 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"workFrom"}, gs::Direction::Out,
        (label_id_t) 10, (label_id_t) 5);
    auto left_left_right_ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_left_right_ctx0),
            std::move(left_left_right_edge_expand_opt0));

    auto left_left_right_get_v_opt1 =
        make_getv_opt(gs::VOpt::End, std::array<label_id_t, 1>{(label_id_t) 5});
    auto left_left_right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_left_right_ctx1),
            std::move(left_left_right_get_v_opt1));
    auto left_left_right_edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto left_left_right_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(left_left_right_ctx2),
            std::move(left_left_right_edge_expand_opt2));

    auto left_left_left_ctx7 =
        Engine::template Join<0, 0, gs::JoinKind::LeftOuterJoin>(
            std::move(left_left_left_ctx6), std::move(left_left_right_ctx3));
    auto left_left_left_ctx8 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_ctx7),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_expr<3, 3, 2, 4>(
                       IC1left_left_left_expr2(),
                       gs::PropertySelector<grape::EmptyType>("None"),
                       gs::PropertySelector<std::string_view>("name"),
                       gs::PropertySelector<int32_t>("workFrom"),
                       gs::PropertySelector<std::string_view>("name"))});
    GroupKey<0, grape::EmptyType> left_left_left_group_key8(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<1, grape::EmptyType> left_left_left_group_key9(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_left_agg_func10 =
        gs::make_aggregate_prop<gs::AggFunc::TO_LIST>(
            std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
            std::integer_sequence<int32_t, 2>{});

    auto left_left_left_ctx9 = Engine::GroupBy(
        graph, std::move(left_left_left_ctx8),
        std::tuple{left_left_left_group_key8, left_left_left_group_key9},
        std::tuple{left_left_left_agg_func10});

    auto left_right_expr0 = gs::make_filter(IC1left_right_expr0());
    double t7 = -grape::GetCurrentTime();
    auto left_right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(left_right_expr0));
    t7 += grape::GetCurrentTime();
    scan_time_3 += t7;

    auto left_right_edge_expand_opt0 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"classYear"}, gs::Direction::Out,
        (label_id_t) 14, (label_id_t) 5);
    auto left_right_ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(left_right_ctx0),
            std::move(left_right_edge_expand_opt0));

    auto left_right_get_v_opt1 = make_getv_opt(
        gs::VOpt::End,
        std::array<label_id_t, 8>{
            (label_id_t) 0, (label_id_t) 1, (label_id_t) 2, (label_id_t) 3,
            (label_id_t) 4, (label_id_t) 5, (label_id_t) 6, (label_id_t) 7});
    auto left_right_ctx2 =
        Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(left_right_ctx1),
            std::move(left_right_get_v_opt1));
    auto left_right_edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto left_right_ctx3 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(2)>(
            graph, std::move(left_right_ctx2),
            std::move(left_right_edge_expand_opt2));

    auto left_left_left_ctx10 =
        Engine::template Join<0, 0, gs::JoinKind::LeftOuterJoin>(
            std::move(left_left_left_ctx9), std::move(left_right_ctx3));
    auto left_left_left_ctx11 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_ctx10),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_expr<4, 4, 3, 5>(
                       IC1left_left_left_expr5(),
                       gs::PropertySelector<grape::EmptyType>("None"),
                       gs::PropertySelector<std::string_view>("name"),
                       gs::PropertySelector<int32_t>("classYear"),
                       gs::PropertySelector<std::string_view>("name"))});
    GroupKey<0, grape::EmptyType> left_left_left_group_key12(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<1, grape::EmptyType> left_left_left_group_key13(
        gs::PropertySelector<grape::EmptyType>("None"));

    GroupKey<2, grape::EmptyType> left_left_left_group_key14(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto left_left_left_agg_func15 =
        gs::make_aggregate_prop<gs::AggFunc::TO_LIST>(
            std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
            std::integer_sequence<int32_t, 3>{});

    auto left_left_left_ctx12 = Engine::GroupBy(
        graph, std::move(left_left_left_ctx11),
        std::tuple{left_left_left_group_key12, left_left_left_group_key13,
                   left_left_left_group_key14},
        std::tuple{left_left_left_agg_func15});

    auto right_expr0 = gs::make_filter(IC1right_expr0());
    double t8 = -grape::GetCurrentTime();
    auto right_ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(right_expr0));
    t8 += grape::GetCurrentTime();
    scan_time_4 += t8;

    auto right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);
    auto right_ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(right_ctx0), std::move(right_edge_expand_opt0));

    auto left_left_left_ctx13 =
        Engine::template Join<0, 0, gs::JoinKind::InnerJoin>(
            std::move(left_left_left_ctx12), std::move(right_ctx1));
    auto left_left_left_ctx14 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(left_left_left_ctx13),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<Date>("birthday")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("gender")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("browserUsed")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("locationIP")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("email")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("language")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(4)>(
                       gs::PropertySelector<std::string_view>("name")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(3)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>(""))});

    for (auto iter : left_left_left_ctx14) {
      auto tuple = iter.GetAllElement();
      output.put_long(std::get<0>(tuple));          // id
      output.put_string_view(std::get<1>(tuple));   // lastName
      output.put_int(std::get<2>(tuple));           // dist
      output.put_date(std::get<3>(tuple));          // birthday
      output.put_long(std::get<4>(tuple));          // creationDate
      output.put_string_view(std::get<5>(tuple));   // gender
      output.put_string_view(std::get<6>(tuple));   // browserUsed
      output.put_string_view(std::get<7>(tuple));   // locationIP
      output.put_string_view(std::get<8>(tuple));   // email
      output.put_string_view(std::get<9>(tuple));   // language
      output.put_string_view(std::get<10>(tuple));  // cityname

      auto& univs = std::get<11>(tuple);
      output.put_int(univs.size());
      for (auto& u : univs) {
        output.put_string_view(std::get<0>(u));  // name
        output.put_int(std::get<1>(u));          // classYear
        output.put_string_view(std::get<2>(u));  // city
      }

      auto& companies = std::get<12>(tuple);
      output.put_int(companies.size());
      for (auto& c : companies) {
        output.put_string_view(std::get<0>(c));  // name
        output.put_int(std::get<1>(c));          // classYear
        output.put_string_view(std::get<2>(c));  // country
      }
    }
  }

  void Query(const gs::MutableCSRInterface& graph,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) {
    oid_t id = input.get<oid_t>("personIdQ1");
    std::string firstName = input.get<std::string>("firstName");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(firstName);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("friendId", output_decoder.get_long());           // id
      node.put("friendLastName", output_decoder.get_string());   // lastName"
      node.put("distanceFromPerson", output_decoder.get_int());  // dist
      node.put("friendBirthday", output_decoder.get_long());     // birthday
      node.put("friendCreationDate",
               output_decoder.get_long());  // creationDate
      node.put("friendGender",
               output_decoder.get_string());  // gender
      node.put("friendBrowserUsed",
               output_decoder.get_string());                      // browserUsed
      node.put("friendLocationIp", output_decoder.get_string());  // locationIP

      boost::property_tree::ptree emails_node;
      std::vector<std::string> emails_list =
          split(output_decoder.get_string(), ';');
      for (auto& str : emails_list) {
        emails_node.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("friendEmails", emails_node);

      boost::property_tree::ptree languages_node;
      std::vector<std::string> languages_list =
          split(output_decoder.get_string(), ';');
      for (auto& str : languages_list) {
        languages_node.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("friendLanguages", languages_node);
      node.put("friendCityName", output_decoder.get_string());  // cityName

      boost::property_tree::ptree univs_node;
      int univs_num = output_decoder.get_int();
      for (int i = 0; i < univs_num; ++i) {
        boost::property_tree::ptree univ_node;
        univ_node.put("organizationName",
                      output_decoder.get_string());       // "universityName"
        univ_node.put("year", output_decoder.get_int());  // "universityYear"
        univ_node.put("placeName", output_decoder.get_string());

        univs_node.push_back(std::make_pair("", univ_node));
      }
      node.add_child("friendUniversities", univs_node);

      boost::property_tree::ptree companies_node;
      int companies_num = output_decoder.get_int();
      for (int i = 0; i < companies_num; ++i) {
        boost::property_tree::ptree company_node;
        company_node.put("organizationName",
                         output_decoder.get_string());       // "companyName"
        company_node.put("year", output_decoder.get_int());  // "companyYear"
        company_node.put("placeName", output_decoder.get_string());

        companies_node.push_back(std::make_pair("", company_node));
      }
      node.add_child("friendCompanies", companies_node);

      output.push_back(std::make_pair("", node));
    }
  }
};

}  // namespace gs

#endif  // IC1_H