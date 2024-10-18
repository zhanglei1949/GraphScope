#include "flex/engines/graph_db/app/cypher_app_utils.h"

#include <sys/wait.h>  // for waitpid()
#include <unistd.h>    // for fork() and execvp()
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace gs {
bool generate_plan(
    const std::string& query,
    std::unordered_map<std::string, physical::PhysicalPlan>& plan_cache) {
  // dump query to file
  static const char* const GRAPHSCOPE_DIR = "/root/0819/GraphScope/";
  static const char* const COMPILER_CONFIG_PATH =
      "/root/0819/GraphScope/flex/tests/hqps/engine_config_test.yaml";

  auto id = std::this_thread::get_id();
  std::stringstream ss;
  ss << id;
  std::string thread_id = ss.str();
  const std::string query_file = "/tmp/temp" + thread_id + ".cypher";
  const std::string output_file = "/tmp/temp" + thread_id + ".pb";
  const std::string jar_path = std::string(GRAPHSCOPE_DIR) +
                               "/interactive_engine/compiler/target/"
                               "compiler-0.0.1-SNAPSHOT.jar:" +
                               std::string(GRAPHSCOPE_DIR) +
                               "/interactive_engine/compiler/target/libs/*";
  const std::string djna_path =
      std::string("-Djna.library.path=") + std::string(GRAPHSCOPE_DIR) +
      "/interactive_engine/executor/ir/target/release/";
  {
    std::ofstream out(query_file);
    out << query;
    out.close();
  }

  // call compiler to generate plan
  {
    pid_t pid = fork();

    if (pid == -1) {
      std::cerr << "Fork failed!" << std::endl;
      return false;
    } else if (pid == 0) {
      const char* const args[] = {
          "java",
          "-cp",
          jar_path.c_str(),
          "-Dgraph.schema=\"\"",
          djna_path.c_str(),
          "com.alibaba.graphscope.common.ir.tools.GraphPlanner",
          COMPILER_CONFIG_PATH,
          query_file.c_str(),
          output_file.c_str(),
          "/tmp/temp.cypher.yaml",
          nullptr  // execvp expects a null-terminated array
      };

      execvp(args[0], const_cast<char* const*>(args));
      std::cerr << "Exec failed!" << std::endl;
      return false;
    } else {
      int status;
      waitpid(pid, &status, 0);
      if (WIFEXITED(status)) {
        std::cout << "Child exited with status " << WEXITSTATUS(status)
                  << std::endl;
      }
    }
  }

  {
    std::ifstream file(output_file, std::ios::binary);

    if (!file.is_open()) {
      return false;
    }

    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::string buffer;
    buffer.resize(size);

    file.read(&buffer[0], size);

    file.close();
    physical::PhysicalPlan plan;
    if (!plan.ParseFromString(std::string(buffer))) {
      return false;
    }

    plan_cache[query] = plan;
  }
  // clean up temp files
  {
    unlink(output_file.c_str());
    unlink(query_file.c_str());
    // unlink("/tmp/temp.cypher.yaml");
    // unlink("/tmp/temp.cypher.yaml_extra_config.yaml");
  }

  return true;
}

void parse_params(std::string_view sw,
                  std::map<std::string, std::string>& params) {
  std::string key, value;
  size_t i = 0;
  while (i < sw.size()) {
    size_t begin = i;
    for (; i < sw.size(); ++i) {
      if (sw[i] == '=') {
        key = std::string(sw.substr(begin, i - begin));
        break;
      }
    }
    begin = ++i;
    for (; i < sw.size(); ++i) {
      if (sw[i] == '&') {
        value = std::string(sw.substr(begin, i - begin));
        break;
      }
    }
    if (i == sw.size()) {
      value = std::string(sw.substr(begin, i - begin));
    }
    i++;
    params[key] = value;
  }
}

}  // namespace gs