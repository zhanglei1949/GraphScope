#ifndef CODEGEN_PROXY_H
#define CODEGEN_PROXY_H

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "glog/logging.h"

#include "proto_generated_gie/job_service.pb.h"
#include "proto_generated_gie/physical.pb.h"

namespace snb {
namespace ic {

// Manages the codegen runner, process the incoming adhoc query, and output to
// the desired directory

class CodegenProxy {
 public:
  static CodegenProxy& get();
  CodegenProxy() : initialized_(false){};

  ~CodegenProxy() = default;

  bool Initialized() { return initialized_; }

  void Init(std::string working_dir, std::string codegen_bin) {
    working_directory_ = working_dir;
    codegen_bin_ = codegen_bin;
    initialized_ = true;
    LOG(INFO) << "CodegenProxy working dir: " << working_directory_
              << ",codegen bin " << codegen_bin_;
  }

  // Do gen
  std::optional<std::pair<int32_t, std::string>> do_gen(
      const physical::PhysicalPlan& plan) {
    LOG(INFO) << "Start generating for query: ";
    auto next_job_id = getNextJobId();
    auto work_dir = get_work_directory(next_job_id);
    auto query_name = "query_" + std::to_string(next_job_id);
    std::string plan_path = prepare_next_job_dir(work_dir, query_name, plan);
    if (plan_path.empty()) {
      return {};
    }

    std::string res_lib_path =
        call_codegen_cmd(plan_path, query_name, work_dir);

    // check res_lib_path exists
    if (!std::filesystem::exists(res_lib_path)) {
      LOG(ERROR) << "res lib path " << res_lib_path << " not exists";
      return {};
    }
    return std::make_pair(next_job_id, res_lib_path);
  }

  std::string call_codegen_cmd(const std::string& plan_path,
                               const std::string& query_name,
                               const std::string& work_dir) {
    // TODO: different suffix for different platform
    std::string res_lib_path = work_dir + "/lib" + query_name + ".so";
    std::string cmd = codegen_bin_ + " -i=" + plan_path + " -w=" + work_dir;
    LOG(INFO) << "Start call codegen cmd: " << cmd;
    auto res = std::system(cmd.c_str());
    if (res != 0) {
      LOG(ERROR) << "call codegen cmd failed: " << cmd;
      return "";
    }
    return res_lib_path;
  }

 private:
  int32_t getNextJobId() { return next_job_id_.fetch_add(1); }

  std::string get_work_directory(int32_t job_id) {
    std::string work_dir = working_directory_ + "/" + std::to_string(job_id);
    ensure_dir_exists(work_dir);
    return work_dir;
  }

  void ensure_dir_exists(const std::string& working_dir) {
    LOG(INFO) << "Ensuring [" << working_dir << "] exists ";
    std::filesystem::path path = working_dir;
    if (!std::filesystem::exists(path)) {
      LOG(INFO) << path << " not exists";
      auto res = std::filesystem::create_directories(path);
      if (!res) {
        LOG(WARNING) << "create " << path << " failed";
      } else {
        LOG(INFO) << "create " << path << " success";
      }
    } else {
      LOG(INFO) << working_dir << " already exists";
    }
  }

  void clear_dir(const std::string& working_dir) {
    LOG(INFO) << "[Cleaning]" << working_dir;
    std::filesystem::path path = working_dir;
    if (std::filesystem::exists(path)) {
      size_t num = 0;
      for (const auto& entry :
           std::filesystem::directory_iterator(working_dir)) {
        std::filesystem::remove_all(entry.path());
        num += 1;
      }
      LOG(INFO) << "remove " << num << "files under " << path;
    }
  }

  std::string prepare_next_job_dir(const std::string& plan_work_dir,
                                   const std::string& query_name,
                                   const physical::PhysicalPlan& plan) {
    // clear directory;
    clear_dir(plan_work_dir);

    // dump plan to file
    std::string plan_path = plan_work_dir + "/" + query_name + ".pb";
    std::ofstream ofs(plan_path, std::ios::binary);
    auto ret = plan.SerializeToOstream(&ofs);
    LOG(INFO) << "Dump plan to: " << plan_path
              << ", ret: " << std::to_string(ret);
    if (!ret) {
      return "";
    }

    return plan_path;
  }

  std::string working_directory_;
  std::string codegen_bin_;
  std::atomic<int32_t> next_job_id_{0};
  bool initialized_;
};

}  // namespace ic

}  // namespace snb

#endif  // CODEGEN_PROXY_H