#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "grape/config.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"

// #include "apps/java_pie/java_pie_default_app.h"
#include "apps/java_pie/java_pie_projected_default_app.h"
#include "core/io/property_parser.h"

#include "giraph_fragment_loader.h"
#include "java_loader_invoker.h"
#include "utils.h"

namespace grape {
using GRAPH_TYPE =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

// using LOADER_TYPE = grape::GiraphFragmentLoader<GRAPH_TYPE>;

void Init(const std::string& params) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
  verifyClasses(params);
}

std::shared_ptr<GRAPH_TYPE> LoadGiraphFragment(
    const grape::CommSpec& comm_spec, const std::string& vfile,
    const std::string& efile, const std::string& vertex_input_format_class,
    const std::string& ipc_socket, bool directed) {
  // construct graph info
  auto graph = std::make_shared<gs::detail::Graph>();
  graph->directed = directed;
  graph->generate_eid = false;

  auto vertex = std::make_shared<gs::detail::Vertex>();
  vertex->label = "label1";
  vertex->vid = "0";
  vertex->protocol = "giraph";
  vertex->values = vfile;
  vertex->values += "#";
  vertex->values += vertex_input_format_class;

  graph->vertices.push_back(vertex);

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  VLOG(1) << "Connected to IPCServer: " << ipc_socket;

  vineyard::ObjectID fragment_id;
  {
    auto loader = std::make_unique<
        gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                vineyard::property_graph_types::VID_TYPE>>(
        client, comm_spec, graph);
    fragment_id = boost::leaf::try_handle_all(
        [&loader]() { return loader->LoadFragment(); },
        [](const vineyard::GSError& e) {
          LOG(ERROR) << e.error_msg;
          return 0;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(ERROR) << "Unmatched error " << unmatched;
          return 0;
        });
  }

  VLOG(1) << "[worker-" << comm_spec.worker_id()
          << "] loaded graph to vineyard ...";

  MPI_Barrier(comm_spec.comm());
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<GRAPH_TYPE>(client.GetObject(fragment_id));
  return fragment;

  //   Run(client, comm_spec, fragment_id, run_projected, run_property,
  //   app_name); MPI_Barrier(comm_spec.comm());
}

void CreateAndQuery(std::string params) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  ptree pt;
  string2ptree(params, pt);

  std::string efile = getFromPtree<std::string>(pt, OPTION_EFILE);
  std::string vfile = getFromPtree<std::string>(pt, OPTION_VFILE);
  std::string vertex_input_format_class =
      getFromPtree<std::string>(pt, OPTION_VERTEX_INPUT_FORMAT_CLASS);
  std::string ipc_socket = getFromPtree<std::string>(pt, OPTION_IPC_SOCKET);
  bool directed = getFromPtree<bool>(pt, OPTION_DIRECTED);
  VLOG(1) << "efile: " << efile << ", vfile: " << vfile
          << " vifc: " << vertex_input_format_class << "directed: " << directed;
  if (efile.empty() || vfile.empty()) {
    LOG(FATAL) << "Make sure efile and vfile are avalibale";
  }

  std::shared_ptr<GRAPH_TYPE> fragment;
  fragment = LoadGiraphFragment(
      comm_spec, efile, vfile, vertex_input_format_class, ipc_socket, directed);

  VLOG(1) << fragment->fid()
          << ",total vertex num: " << fragment->GetTotalVerticesNum()
          << "frag vnum: " << fragment->GetVerticesNum();
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace grape
