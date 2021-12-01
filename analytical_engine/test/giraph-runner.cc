#include <memory>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

DEFINE_string(input_format_class, "", "java class defines the input format");
DEFINE_string(output_format_class, "", "java class defines the output format");
DEFINE_string(app_class, "", "app class to run");
DEFINE_string(input_vfile, "", "input vertex file");
DEFINE_string(input_efile, "", "input edge file");
DEFINE_string(output_path, "", "output file path");
DEFINE_string(aggregator_class, "", "aggregator class");
DEFINE_string(combiner_class, "", "combiner for message processing");
DEFINE_string(resolver_class, "", "resolver for graph loading");
DEFINE_string(lib_path, "",
              "path for dynamic lib where the desired entry function exists");

typedef void* RunT(std::string args);

// put all flags in a json str
std::string flags2JsonStr() { return ""; }

class GiraphJobRunner {
 public:
  GiraphJobRunner(const std::string& lib_path)
      : lib_path_(lib_path), dl_handle_(nullptr), run_handle_(nullptr) {}
  bool Init() {
    BOOST_LEAF_ASSIGN(dl_handle_, open_lib(lib_path_.c_str()));

    BOOST_LEAF_AUTO(function_ptr, get_func_ptr(lib_path_, dl_handle_, "Run"));
    if (function_ptr) {
      run_handle_ = reinterpret_cast<RunT*>(function_ptr);
    }
    if (run_handle_) {
      return true;
    }
    return false;
  }

  void Run(std::string params) { run_handle_(std::move(params)); }

 private:
  void* dl_handle_;
  std::string lib_path_;
  RunT* run_handle_;
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./giraph_runner [options]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "giraph-runner");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("giraph-runner");
  google::InstallFailureSignalHandler();

  VLOG(1) << "Finish option parsing";

  std::string lib_path = FLAGS_lib_path;
  GiraphJobRunner runner(lib_path);
  if (runner.Init()) {
    std::string params = flags2JsonStr();
    runner.Run(std::move(params));
    VLOG(1) << "Finish Querying.";
  } else {
    VLOG(1) << "Exiting since error in init.";
  }

  google::ShutdownGoogleLogging();
}
