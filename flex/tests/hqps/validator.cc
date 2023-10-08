#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include "flex/tests/hqps/ic/ic1.h"
#include "flex/tests/hqps/ic/ic10.h"
#include "flex/tests/hqps/ic/ic11.h"
#include "flex/tests/hqps/ic/ic12.h"
#include "flex/tests/hqps/ic/ic2.h"
#include "flex/tests/hqps/ic/ic3.h"
#include "flex/tests/hqps/ic/ic4.h"
#include "flex/tests/hqps/ic/ic5.h"
#include "flex/tests/hqps/ic/ic6.h"
// #include "flex/tests/hqps/ic/ic7.h"
#include "flex/tests/hqps/ic/ic8.h"
#include "flex/tests/hqps/ic/ic9.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "glog/logging.h"

void convert_ptree(const boost::property_tree::ptree& pt,
                   const std::string& prefix,
                   std::vector<std::pair<std::string, std::string>>& vec) {
  if (pt.empty()) {
    vec.emplace_back(prefix, pt.data());
  } else {
    for (auto& pair : pt) {
      auto new_prefix = prefix + "|" + pair.first;
      convert_ptree(pair.second, new_prefix, vec);
    }
  }
}

bool same_property_tree(const boost::property_tree::ptree& lhs,
                        const boost::property_tree::ptree& rhs) {
  std::vector<std::pair<std::string, std::string>> lhs_vec, rhs_vec;
  convert_ptree(lhs, "", lhs_vec);
  convert_ptree(rhs, "", rhs_vec);
  std::sort(lhs_vec.begin(), lhs_vec.end());
  std::sort(rhs_vec.begin(), rhs_vec.end());
  return lhs_vec == rhs_vec;
}

template <typename APP_T>
void validate(const gs::MutableCSRInterface& graph,
              const std::string& filename) {
  APP_T app;
  auto ts = std::numeric_limits<int64_t>::max() - 1;

  FILE* fin = fopen(filename.c_str(), "r");
  const int kMaxLineSize = 1048576;
  char line[kMaxLineSize];
  double t0 = -grape::GetCurrentTime();
  size_t cnt = 0;
  size_t success = 0;
  while (fgets(line, kMaxLineSize, fin)) {
    char* ptr = strchr(line, '|');

    std::string input_str(line, ptr - line);
    std::string output_str(ptr + 1, strnlen(ptr + 1, kMaxLineSize));

    std::stringstream ssinput(input_str);
    std::stringstream ssexpected_output(output_str);

    boost::property_tree::ptree ssinput_pt;
    boost::property_tree::read_json(ssinput, ssinput_pt);

    boost::property_tree::ptree ssexpected_output_pt;
    boost::property_tree::read_json(ssexpected_output, ssexpected_output_pt);

    boost::property_tree::ptree ssoutput_pt;
    app.Query(graph, ssinput_pt, ssoutput_pt);
    if (!same_property_tree(ssoutput_pt, ssexpected_output_pt)) {
      LOG(INFO) << "Wrong answer when validating " << filename
                << "on case: " << cnt;
      LOG(INFO) << "Input: ";
      std::stringstream input_ss;
      boost::property_tree::json_parser::write_json(input_ss, ssinput_pt);
      LOG(INFO) << input_ss.str();
      LOG(INFO) << "Output: ";
      std::stringstream output_ss;
      boost::property_tree::json_parser::write_json(output_ss, ssoutput_pt);
      LOG(INFO) << output_ss.str();
      LOG(INFO) << "Expected output: ";
      std::stringstream expected_output_ss;
      boost::property_tree::json_parser::write_json(expected_output_ss,
                                                    ssexpected_output_pt);
      LOG(INFO) << expected_output_ss.str();
      // LOG(FATAL) << "Exited";
    } else {
      LOG(INFO) << "Correct answer when validating <" << input_str << ">";
      success += 1;
    }

    cnt += 1;
  }

  t0 += grape::GetCurrentTime();
  LOG(INFO) << "validate: " << filename << " times: " << cnt
            << ", avg time : " << (t0 / cnt) << "s, success/total: " << success
            << "/" << cnt;
}

void validate_all(const gs::MutableCSRInterface& graph,
                  const std::string& validate_dir) {
  LOG(INFO) << "Finish validating all tests";
  //  validate<gs::IC1>(graph, validate_dir + "/validation_params_ic1.csv");
  //  validate<gs::IC2>(graph, validate_dir + "/validation_params_ic2.csv");
  // validate<gs::IC3>(graph, validate_dir + "/validation_params_ic3.csv");
  // validate<gs::IC4>(graph, validate_dir + "/validation_params_ic4.csv");
  // validate<gs::IC5>(graph, validate_dir + "/validation_params_ic5.csv");
  // validate<gs::IC6>(graph, validate_dir + "/validation_params_ic6.csv");
  // validate<gs::IC7>(graph, validate_dir + "/validation_params_ic7.csv");
  // validate<gs::IC8>(graph, validate_dir + "/validation_params_ic8.csv");
  // validate<gs::IC9>(graph, validate_dir + "/validation_params_ic9.csv");
  validate<gs::IC10>(graph, validate_dir + "/validation_params_ic10.csv");
  //  validate<gs::IC11>(graph, validate_dir + "/validation_params_ic11.csv");
  // validate<gs::IC12>(graph, validate_dir + "/validation_params_ic12.csv");
}

int main(int argc, char** argv) {
  if (argc < 3) {
    LOG(INFO) << "Usage: " << argv[0] << " <validate_dir> <work_dir>";
    return 0;
  }

  std::string validate_dir = argv[1];
  std::string work_dir = argv[2];
  auto& db = gs::GraphDB::get();
  db.Init(work_dir, 1);
  LOG(INFO) << "Finish Deserialize graph.";
  auto& sess = gs::GraphDB::get().GetSession(0);
  gs::MutableCSRInterface graph_store(sess);
  validate_all(graph_store, validate_dir);

  return 0;
}