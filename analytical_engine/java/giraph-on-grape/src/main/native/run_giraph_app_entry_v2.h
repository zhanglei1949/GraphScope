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
#include "vineyard/client/client.h"

// #include "apps/java_pie/java_pie_default_app.h"
#include "apps/java_pie/java_pie_projected_default_app.h"
#include "core/io/property_parser.h"
#include "core/loader/arrow_fragment_loader.h"

#include "giraph_fragment_loader.h"
#include "java_loader_invoker.h"
#include "utils.h"

#define QUOTE(X) #X

#if !defined(GIRAPH_OID_TYPE)
#error "Missing GIRAPH_OID_TYPE"
#endif

namespace grape {
using FragmentType =
    vineyard::ArrowFragment<GIRAPH_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
using FragmentLoaderType =
    gs::ArrowFragmentLoader<GIRAPH_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
// using LOADER_TYPE = grape::GiraphFragmentLoader<FragmentType>;

void Init(const std::string& params) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
  verifyClasses(params);
}

vineyard::ObjectID LoadGiraphFragment(
    const grape::CommSpec& comm_spec, const std::string& vfile,
    const std::string& efile, const std::string& vertex_input_format_class,
    vineyard::Client& client,  bool directed, const std::string params) {
  // construct graph info
  auto graph = std::make_shared<gs::detail::Graph>();
  graph->directed = directed;
  graph->generate_eid = false;

  auto vertex = std::make_shared<gs::detail::Vertex>();
  vertex->label = "label1";
  vertex->vid = "0";
  vertex->protocol = "giraph";
  vertex->values = vfile;
  // vertex->values += "#";
  // vertex->values += vertex_input_format_class;
  vertex->values += "#";
  vertex->values += params;
  // VLOG(1) << "vertex->values " << vertex->values;

  graph->vertices.push_back(vertex);

  auto edge = std::make_shared<gs::detail::Edge>();
  edge->label = "label2";
  auto subLabel = std::make_shared<gs::detail::Edge::SubLabel>();
  subLabel->src_label = "label1";
  subLabel->src_vid = "0";
  subLabel->dst_label = "label1";
  subLabel->dst_vid = "0";
  subLabel->protocol = "giraph";
  subLabel->values = efile;  // shall not be used
  edge->sub_labels.push_back(*subLabel.get());

  graph->edges.push_back(edge);



  vineyard::ObjectID fragment_id;
  {
    auto loader =
        std::make_unique<FragmentLoaderType>(client, comm_spec, graph);
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

  VLOG(1) << "[worker-" << comm_spec.worker_id() << "] got fragment "
          << fragment_id;
  return fragment_id;

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

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));
  VLOG(1) << "Connected to IPCServer: " << ipc_socket;

  auto fragment_id =
      LoadGiraphFragment(comm_spec, efile, vfile, vertex_input_format_class,
                         client, directed, params);
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

  // VLOG(1) << fragment->fid()
  //       << ",total vertex num: " << fragment->GetTotalVerticesNum()
  //       << "frag vnum: " << fragment->GetVerticesNum(0);
  VLOG(1) << "fid: " << fragment->fid() << "fnum: " << fragment->fnum()
          << "v label num: " << fragment->vertex_label_num()
          << "e label num: " << fragment->edge_label_num()
          << "total v num: " << fragment->GetTotalVerticesNum();
  VLOG(1) << "inner vertices: " << fragment->GetInnerVerticesNum(0);
  //    VLOG(1) <<" frag vnum: " << fragment->GetVerticesNum(0);
  /*
    for (int v_label = 0; v_label < fragment->vertex_label_num(); ++v_label) {
      auto iv = fragment->InnerVertices(v_label);
      VLOG(10) << "iv size: " << iv.size();
      for (auto v : fragment->InnerVertices(v_label)) {
        VLOG(10) << "v: " << v.GetValue();
        if (v.GetValue() < 5) {
          for (int e_label = 0; e_label < fragment->vertex_label_num();
               ++e_label) {
              auto edges = fragment->GetOutgoingAdjList(v, e_label);
              VLOG(10) << "e: " << edges.size();
            for (auto edge : edges) {
              VLOG(10) << "v: " << v.GetValue()
                       << " dst: " << edge.neighbor().GetValue();
            }
          }
        }
      }
    }
  */
}
void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace grape
