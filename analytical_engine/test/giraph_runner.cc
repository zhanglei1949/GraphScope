#include <unistd.h>
#include <memory>
#include <string>
#include <utility>
#include <dlfcn.h>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include <boost/leaf/error.hpp>

#include "grape/config.h"

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
DEFINE_string(loading_thread_num, "",
              "number of threads will be used in loading the graph");

inline void* open_lib(const char* path) {
  void* handle = dlopen(path, RTLD_LAZY);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
      LOG(ERROR) << "Error in open library: " <<path << p_error_msg;
      return nullptr;
  }
  return handle;
}

inline void* get_func_ptr(const std::string& lib_path, void* handle,
                                      const char* symbol) {
  auto* p_func = dlsym(handle, symbol);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
      LOG(ERROR) << "Failed to get symbol" << symbol << " from " << p_error_msg;
  }
  return p_func;
}


typedef void* RunT(std::string args);

// put all flags in a json str
std::string flags2JsonStr() {
  boost::property_tree::ptree pt;
  pt.put("input_format_class", FLAGS_input_format_class);
  pt.put("output_format_class", FLAGS_output_format_class);
  pt.put("app_class", FLAGS_app_class);
  pt.put("input_vfile", FLAGS_input_vfile);
  pt.put("input_efile", FLAGS_input_efile);
  pt.put("output_path", FLAGS_output_path);
  pt.put("aggregator_class", FLAGS_aggregator_class);
  pt.put("combiner_class", FLAGS_combiner_class);
  pt.put("resolver_class", FLAGS_resolver_class);
  pt.put("lib_path", FLAGS_lib_path);
  pt.put("loading_thread_num", FLAGS_loading_thread_num);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  return std::move(ss.str());
}

class GiraphJobRunner {
 public:
  GiraphJobRunner(const std::string& lib_path)
      : lib_path_(lib_path), dl_handle_(nullptr), run_handle_(nullptr) {}
  bool Init() {
    dl_handle_ = open_lib(lib_path_.c_str());
    if (!dl_handle_){
       return false;
    }
    auto function_ptr = get_func_ptr(lib_path_, dl_handle_, "Run");
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
  std::string lib_path_;
  void* dl_handle_;
  RunT* run_handle_;
};

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
