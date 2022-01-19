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

#include "apps/java_pie/java_pie_default_app.h"

#include "giraph_fragment_loader.h"
#include "java_loader_invoker.h"
#include "utils.h"

#define QUOTE(X) #X

#if !defined(GRAPH_TYPE)
#error "Missing GRAPH_TYPE"
#endif

#if !defined(APP_TYPE)
#error "Missing macro APP_TYPE"
#endif

namespace grape {

using oid_t = GRAPH_TYPE::oid_t;
using vdata_t = GRAPH_TYPE::vdata_t;
using edata_t = GRAPH_TYPE::edata_t;
using vid_t = uint64_t;

using LOADER_TYPE = grape::GiraphFragmentLoader<GRAPH_TYPE>;

void Init(const std::string& params) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
  verifyClasses(params);
}

// Load from ImmutableEdgecutFragmentLoader
std::shared_ptr<GRAPH_TYPE> loadWithGrapeLoader(
    const grape::CommSpec& comm_spec, const ptree& pt, const std::string& vfile,
    const std::string& efile) {
  LoadGraphSpec graph_spec = DefaultLoadGraphSpec();
  bool directed = getFromPtree<bool>(pt, OPTION_DIRECTED);
  graph_spec.set_directed(directed);
  graph_spec.set_rebalance(false, 0);

  bool deserialize = getFromPtree<bool>(pt, OPTION_DESERIALIZE);
  bool serialize = getFromPtree<bool>(pt, OPTION_SERIALIZE);
  std::string serialize_prefix =
      getFromPtree<std::string>(pt, OPTION_SERIALIZE_PREFIX);
  VLOG(1) << "directed: " << directed << ", Serialize: " << serialize
          << ", Deserialize: " << deserialize
          << ", Prefix: " << serialize_prefix;
  if (deserialize) {
    CHECK(!serialize_prefix.empty());
    graph_spec.set_deserialize(true, serialize_prefix);
  } else if (serialize) {
    CHECK(!serialize_prefix.empty());
    graph_spec.set_serialize(true, serialize_prefix);
  }
  // if deserialzation enabled, do deserialzation
  return LoadGraph<GRAPH_TYPE, HashPartitioner<typename GRAPH_TYPE::oid_t>>(
      efile, vfile, comm_spec, graph_spec);
}

std::shared_ptr<GRAPH_TYPE> loadWithGiraphLoader(
    const grape::CommSpec& comm_spec, const ptree& pt, const std::string& vfile,
    const std::string& efile) {
  int loading_threads_num = getFromPtree<int>(pt, OPTION_LOADING_THREAD_NUM);
  if (loading_threads_num <= 0) {
    LOG(ERROR) << "Invalid loading thread num: " << loading_threads_num;
  }
  int vertex_buffer_nums = loading_threads_num * comm_spec.fnum();
  int edge_buffer_nums =
      loading_threads_num * comm_spec.fnum() * comm_spec.fnum();

  std::vector<byte_vector> vid_buffers(vertex_buffer_nums),
      vdata_buffers(vertex_buffer_nums), esrc_id_buffers(edge_buffer_nums),
      edst_id_buffers(edge_buffer_nums), edata_buffers(edge_buffer_nums);
  std::vector<offset_vector> vid_offsets(vertex_buffer_nums),
      esrc_id_offsets(edge_buffer_nums), edst_id_offsets(edge_buffer_nums);

  // Load via java_load_invoker. The class names must be ok since it has been
  // verified.
  // JavaLoaderInvoker java_loader_invoker;
  // java_loader_invoker.Init(
  //     DEFAULT_JAVA_LOADER_CLASS,
  //     DEFAULT_JAVA_LOADER_METHOD_NAME,
  //     DEFAULT_JAVA_LOADER_METHOD_SIG,
  //     getFromPtree<std::string>(pt, OPTION_INPUT_FORMAT_CLASS));
  // // fill in theses buffers in java
  // java_loader_invoker.CallJavaLoader(
  //     vid_buffers, vid_offsets, vdata_buffers, esrc_id_buffers,
  //     esrc_id_offsets, edst_id_buffers, edst_id_offsets, edata_buffers);

  // std::shared_ptr<LOADER_TYPE> loader =
  //     std::make_shared<LOADER_TYPE>(comm_spec);
  // loader->AddVertexBuffers(std::move(vid_buffers), std::move(vid_offsets),
  //                          std::move(vdata_buffers));
  // VLOG(1) << "Finish add vertex buffers";
  // loader->AddEdgeBuffers(std::move(esrc_id_buffers),
  // std::move(esrc_id_offsets),
  //                        std::move(edst_id_buffers),
  //                        std::move(edst_id_offsets),
  //                        std::move(edata_buffers));
  // VLOG(1) << "Finish add edge buffers";
  return std::shared_ptr<GRAPH_TYPE>(nullptr);
}

template <typename FRAG_T>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           const std::string& app_class, const std::string& context_class,
           const std::string& frag_name, const std::string user_lib_path,
           const std::string& params_str) {
  auto app = std::make_shared<APP_TYPE>();
  auto worker = APP_TYPE::CreateWorker(app, fragment);

  auto spec = DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);

  MPI_Barrier(comm_spec.comm());
  double t = -GetCurrentTime();
  grape::Communicator* communicator = new grape::Communicator();
  communicator->InitCommunicator(comm_spec.comm());

  VLOG(2) << "Communicator address:" << reinterpret_cast<jlong>(communicator);
  // No matter we use netty or mpi for java ipc communication, we always need
  // one communicator to provided us will basic comm.
  worker->Query(app_class, context_class, frag_name, user_lib_path, params_str,
                reinterpret_cast<jlong>(communicator));

  t += GetCurrentTime();
  VLOG(1) << "Query time: " << t;

  std::ofstream unused_stream;
  unused_stream.open("empty");
  worker->Output(unused_stream);
  unused_stream.close();
}

void CreateAndQuery(std::string params) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  ptree pt;
  string2ptree(params, pt);

  std::string efile = getFromPtree<std::string>(pt, OPTION_EFILE);
  std::string vfile = getFromPtree<std::string>(pt, OPTION_VFILE);
  VLOG(1) << "efile: " << efile << ", vfile: " << vfile;
  if (efile.empty() || vfile.empty()) {
    LOG(FATAL) << "Make sure efile and vfile are avalibale";
  }

  bool using_grape_loader = getFromPtree<bool>(pt, OPTION_GRAPE_LOADER);
  VLOG(1) << "Using grape loader: " << (using_grape_loader ? "true" : "false");
  std::shared_ptr<GRAPH_TYPE> fragment;
  if (using_grape_loader) {
    fragment = loadWithGrapeLoader(comm_spec, pt, vfile, efile);
  } else {
    fragment = loadWithGiraphLoader(comm_spec, pt, vfile, efile);
  }

  VLOG(1) << fragment->fid() << ",vertex num: " << fragment->GetVerticesNum()
          << ",edge num:" << fragment->GetEdgeNum();
  // return fragment;
  std::string user_app_class = getFromPtree<std::string>(pt, OPTION_APP_CLASS);
  std::string driver_app_class =
      getFromPtree<std::string>(pt, OPTION_DRIVER_APP_CLASS);
  std::string driver_app_context =
      getFromPtree<std::string>(pt, OPTION_DRIVER_CONTEXT_CLASS);
  std::string user_lib_path = getFromPtree<std::string>(pt, OPTION_LIB_PATH);

  //  std::string frag_name = QUOTE(GRAPH_TYPE);
  std::string frag_name = getenv("GRAPH_TYPE");
  CHECK(!frag_name.empty());
  Query<GRAPH_TYPE>(comm_spec, fragment, driver_app_class, driver_app_context,
                    frag_name, user_lib_path, params);
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace grape
