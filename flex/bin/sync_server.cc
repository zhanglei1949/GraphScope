#include <filesystem>
#include <iostream>
#include "stdlib.h"

#include "flex/engines/hqps/server/service.h"

#include "flex/engines/hqps/server/codegen_proxy.h"
#include "flex/engines/hqps/server/stored_procedure.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>

namespace bpo = boost::program_options;

static constexpr const char* CODEGEN_BIN = "load_plan_and_run.sh";

std::string find_codegen_bin() {
  // first check whether flex_home env exists
  std::string flex_home;
  std::string codegen_bin;
  char* flex_home_char = getenv("FLEX_HOME");
  if (flex_home_char == nullptr) {
    // infer flex_home from current binary' directory
    char* bin_path = realpath("/proc/self/exe", NULL);
    std::string bin_path_str(bin_path);
    // flex home should be bin_path/../../
    std::string flex_home_str =
        bin_path_str.substr(0, bin_path_str.find_last_of("/"));
    // usr/loca/bin/
    flex_home_str = flex_home_str.substr(0, flex_home_str.find_last_of("/"));
    // usr/local/

    LOG(INFO) << "infer flex_home as installed, flex_home: " << flex_home_str;
    // check codege_bin_path exists
    codegen_bin = flex_home_str + "/bin/" + CODEGEN_BIN;
    // if flex_home exists, return flex_home
    if (std::filesystem::exists(codegen_bin)) {
      return codegen_bin;
    } else {
      // if not found, try as if it is in build directory
      // flex/build/
      flex_home_str = flex_home_str.substr(0, flex_home_str.find_last_of("/"));
      // flex/
      LOG(INFO) << "infer flex_home as build, flex_home: " << flex_home_str;
      codegen_bin = flex_home_str + "/bin/" + CODEGEN_BIN;
      if (std::filesystem::exists(codegen_bin)) {
        return codegen_bin;
      } else {
        LOG(FATAL) << "codegen bin not exists: ";
        return "";
      }
    }
  } else {
    flex_home = std::string(flex_home_char);
    LOG(INFO) << "flex_home env exists, flex_home: " << flex_home;
    codegen_bin = flex_home + "/bin/" + CODEGEN_BIN;
    if (std::filesystem::exists(codegen_bin)) {
      return codegen_bin;
    } else {
      LOG(FATAL) << "codegen bin not exists: ";
      return "";
    }
  }
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help,h", "Display help messages")(
      "server-config,c", bpo::value<std::string>(),
      "path to server config yaml")("plugin-dir,p", bpo::value<std::string>(),
                                    "directory where plugins exists")(
      "codegen-dir,d",
      bpo::value<std::string>()->default_value("/tmp/codegen/"),
      "codegen working directory")("codegen-bin,b", bpo::value<std::string>(),
                                   "codegen binary path")(
      "graph-yaml,y", bpo::value<std::string>(), "graph yaml path")(
      "db-home", bpo::value<std::string>(), "db home path")(
      "graph-schema", bpo::value<std::string>(),
      "path to graph schema, used by gie")(
      "grape-data-path,g", bpo::value<std::string>(), "grape data path");

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  uint32_t shard_num = 1;
  uint16_t http_port = 10000;
  if (vm.count("server-config") != 0) {
    std::string server_config_path = vm["server-config"].as<std::string>();
    // check file exists
    if (!std::filesystem::exists(server_config_path)) {
      LOG(ERROR) << "server-config not exists: " << server_config_path;
      return 0;
    }
    YAML::Node config = YAML::LoadFile(server_config_path);
    auto dbms_node = config["dbms"];
    if (dbms_node) {
      auto server_node = dbms_node["server"];
      if (!server_node) {
        LOG(ERROR) << "dbms.server config not found";
        return 0;
      }
      auto shard_num_node = server_node["shared_num"];
      if (shard_num_node) {
        shard_num = shard_num_node.as<uint32_t>();
      } else {
        LOG(INFO) << "shared_num not found, use default value 1";
      }
      auto http_port_node = dbms_node["port"];
      if (http_port_node) {
        http_port = http_port_node.as<uint16_t>();
      } else {
        LOG(INFO) << "http_port not found, use default value 10000";
      }
    } else {
      LOG(ERROR) << "dbms config not found";
      return 0;
    }
  } else {
    LOG(INFO) << "server-config is not specified, use default config";
  }
  LOG(INFO) << "shard_num: " << shard_num;
  LOG(INFO) << "http_port: " << http_port;

  std::string codegen_dir = vm["codegen-dir"].as<std::string>();

  LOG(INFO) << "codegen dir: " << codegen_dir;

  std::string codegen_bin;
  if (vm.count("codegen-bin") == 0) {
    LOG(INFO) << "codegen-bin is not specified";
    LOG(INFO) << "Try to find with relative path: ";
    codegen_bin = find_codegen_bin();
  } else {
    LOG(INFO) << "codegen-bin is specified";
    codegen_bin = vm["codegen-bin"].as<std::string>();
  }

  LOG(INFO) << "codegen bin: " << codegen_bin;

  // check codegen bin exists
  if (!std::filesystem::exists(codegen_bin)) {
    LOG(ERROR) << "codegen bin not exists: " << codegen_bin;
    return 0;
  }

  // check cmakeList exists

  // clear codegen dir
  if (std::filesystem::exists(codegen_dir)) {
    LOG(INFO) << "codegen dir exists, clear directory";
    std::filesystem::remove_all(codegen_dir);
  } else {
    // create codegen_dir
    LOG(INFO) << "codegen dir not exists, create directory";
    std::filesystem::create_directory(codegen_dir);
  }

  if (vm.count("grape-data-path")) {
    auto dir = vm["grape-data-path"].as<std::string>();
    if (vm.count("graph-yaml")) {
      auto graph_yaml = vm["graph-yaml"].as<std::string>();
      std::cout << "Start load grape data from " << dir << " with graph yaml "
                << graph_yaml << std::endl;
      gs::GrapeGraphInterface::get().Open(graph_yaml, dir);
    } else {
      std::cout << "Start load grape data from " << dir << std::endl;
      gs::GrapeGraphInterface::get().Open(dir);
    }
  } else {
    std::cout << "grape data path is required" << std::endl;
    return 0;
  }

  // add plugins after graph store init
  if (vm.count("plugin-dir") == 0) {
    LOG(INFO) << "plugin-dir is not specified" << std::endl;
  } else {
    std::string plugin_dir = vm["plugin-dir"].as<std::string>();
    LOG(INFO) << "Load plugins from dir: " << plugin_dir;
    gs::StoredProcedureManager::get().LoadFromPluginDir(plugin_dir);
  }

  snb::ic::CodegenProxy::get().Init(codegen_dir, codegen_bin);

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  snb::ic::service::get().init(shard_num, http_port, false);
  snb::ic::service::get().run_and_wait_for_exit();

  return 0;
}