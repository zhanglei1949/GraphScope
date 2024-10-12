#include "flex/engines/graph_db/app/cypher_app.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

#include <sys/wait.h>  // for waitpid()
#include <unistd.h>    // for fork() and execvp()

namespace gs {

bool CypherReadApp::generate_plan(const std::string& query) {
  // dump query to file
  auto id = std::this_thread::get_id();
  std::stringstream ss;
  ss << id;
  std::string thread_id = ss.str();
  const std::string query_file = "/tmp/temp" + thread_id + ".cypher";
  const std::string output_file = "/tmp/temp" + thread_id + ".pb";
  {
    std::ofstream out(query_file);
    out << query;
    out.close();
  }

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
          "/root/0915/GraphScope/flex/../interactive_engine/compiler/target/"
          "compiler-0.0.1-SNAPSHOT.jar:/root/0915/GraphScope/flex/../"
          "interactive_engine/compiler/target/libs/*",
          "-Dgraph.schema=/root/0627/interactive_workspace/data/ldbc/"
          "graph.yaml",
          "-Djna.library.path=/root/0915/GraphScope/flex/../interactive_engine/"
          "executor/ir/target/release/",
          "com.alibaba.graphscope.common.ir.tools.GraphPlanner",
          "/root/0915/GraphScope/flex/tests/hqps/engine_config_test.yaml",
          query_file.c_str(),
          output_file.c_str(),
          "/tmp/temp.cypher.yaml",
          "/tmp/temp.cypher.yaml_extra_config.yaml",
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
      LOG(ERROR) << "Parse plan failed...";
      return false;
    }

    plan_cache_[query] = plan;
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

bool CypherReadApp::Query(const GraphDBSession& graph, Decoder& input,
                          Encoder& output) {
  auto txn = graph.GetReadTransaction();
  std::string_view bytes = input.get_bytes();

  size_t sep = bytes.find_first_of('?');
  auto query_str = bytes.substr(0, sep);
  auto params_str = bytes.substr(sep + 1);
  std::map<std::string, std::string> params;
  parse_params(params_str, params);
  auto query = std::string(query_str.data(), query_str.size());
  if (plan_cache_.count(query)) {
    // LOG(INFO) << "Hit cache for query ";
  } else {
    for (int i = 0; i < 3; ++i) {
      if (!generate_plan(query)) {
        LOG(ERROR) << "Generate plan failed for query: " << query;
      } else {
        break;
      }
    }
  }

  const auto& plan = plan_cache_[query];

  // LOG(INFO) << "plan: " << plan.DebugString();

  auto ctx = runtime::runtime_eval(plan, txn, params);

  runtime::eval_sink_encoder(ctx, txn, output);
  return true;
}
AppWrapper CypherReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherReadApp(), NULL);
}
}  // namespace gs