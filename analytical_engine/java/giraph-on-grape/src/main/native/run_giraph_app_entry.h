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

// data vector contains all bytes, can be used to hold oid and vdata, edata.
using byte_vector = std::vector<char>;
// offset vector contains offsets to deserialize data vector.
using offset_vector = std::vector<int>;


void Init(const std::string& params) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
  verifyClasses(params);
}

/**
 * @brief Call java loader to fillin buffers
 *
 */
void callJavaLoader(std::vector<byte_vector>& vid_buffers,
                    std::vector<offset_vector>& vid_offsets,
                    std::vector<byte_vector>& vdata_buffers,
                    std::vector<byte_vector>& esrc_id_buffers,
                    std::vector<offset_vector>& esrc_id_offsets,
                    std::vector<byte_vector>& edst_id_buffers,
                    std::vector<offset_vector>& edst_id_offsets,
                    std::vector<byte_vector>& edata_buffers) {}

template <typename FRAG_T>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           std::string& params_str) {
  auto app = std::make_shared<APP_TYPE>();
  auto worker = APP_TYPE::CreateWorker(app, fragment);

  auto spec = DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);

  MPI_Barrier(comm_spec.comm());
  double t = -GetCurrentTime();

  worker->Query(params_str);

  t += GetCurrentTime();
  VLOG(1) << "Query time" << t;
}

void CreateAndQuery(std::string params) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  ptree pt;
  string2ptree(params, pt);

  int loading_threads_num = getFromPtree<int>(pt, "loading_thread_num");
  if (loading_threads_num < 0) {
    LOG(ERROR) << "Invalid loading thread num: " << loading_threads_num;
    return;
  }

  std::shared_ptr<LOADER_TYPE> loader = std::make_shared<LOADER_TYPE>(comm_spec);

  int vertex_buffer_nums = loading_threads_num * comm_spec.fnum();
  int edge_buffer_nums =
      loading_threads_num * comm_spec.fnum() * comm_spec.fnum();

  std::vector<byte_vector> vid_buffers(vertex_buffer_nums),
      vdata_buffers(vertex_buffer_nums), esrc_id_buffers(edge_buffer_nums),
      edst_id_buffers(edge_buffer_nums), edata_buffers(edge_buffer_nums);
  std::vector<offset_vector> vid_offsets(vertex_buffer_nums),
      esrc_id_offsets(edge_buffer_nums), edst_id_offsets(edge_buffer_nums);

  // fill in theses buffers in java
  callJavaLoader(vid_buffers, vid_offsets, vdata_buffers, esrc_id_buffers,
                 esrc_id_offsets, edst_id_buffers, edst_id_offsets,
                 edata_buffers);

  loader->AddVertexBuffers(std::move(vid_buffers), std::move(vid_offsets), std::move(vdata_buffers));
  VLOG(1) << "Finish add vertex buffers";
  loader->AddEdgeBuffers(std::move(esrc_id_buffers), std::move(esrc_id_offsets), std::move(edst_id_buffers),
                         std::move(edst_id_offsets), std::move(edata_buffers));
  VLOG(1) << "Finish add edge buffers";

  std::shared_ptr<GRAPH_TYPE> fragment = loader->LoadFragment();
  //return fragment;
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}
