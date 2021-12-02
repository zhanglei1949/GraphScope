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

#include "giraph_fragment_loader.h"
#include "util.h"

#define QUOTE(X) #X

#if !defined(_GRAPH_TYPE)
#error "Missing _GRAPH_TYPE"
#endif

#if !defined(_APP_TYPE)
#error "Missing macro _APP_TYPE"
#endif

namespace grape {

using oid_t = _GRAPH_TYPE::oid_t;
using vdata_t = _GRAPH_TYPE::vdata_t;
using edata_t = _GRAPH_TYPE::edata_t;
using vid_t = uint64_t;

using LOADER_TYPE = grape::GiraphFragmentLoader<_GRAPH_TYPE>;

// data vector contains all bytes, can be used to hold oid and vdata, edata.
using byte_vector = std::vector<char>;
// offset vector contains offsets to deserialize data vector.
using offset_vector = std::vector<int>;

static constexpr const char* GIRAPH_PARAMS_CHECK_CLASS =
    "org.apache.giraph.utils.GiraphParamsChecker";
static constexpr const char* VERIFY_CLASSES_SIGN =
    "(Ljava/lang/String;Ljava/lang/String;)V";

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

template <FRAG_T>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           std::string& params_str) {
  auto app = std::make_shared<_APP_TYPE>();
  auto worker = _APP_TYPE::CreateWorker(app, fragment);

  auto spec = DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);

  MPI_Barrier(comm_spec.comm());
  double t = -GetCurrentTime();

  worker->Query(std::forward<Args>(args)...);
}

void CreateAndQuery(std::string params) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  ptree pt = string2ptree(params);

  int loading_threads_num = getFromPtree<int>(pt, "loading_thread_num");
  if (loading_threads_num < 0) {
    LOG(ERROR) << "Invalid loading thread num: " << loading_threads_num;
    return;
  }

  std::shared_ptr<LOADER_TYPE> loader = std::make_shared<LOADER_TYPE>();

  int vertex_buffer_nums = loading_threads_num * comm_spec.fnum();
  int edge_buffer_nums =
      loading_threads_num * *comm_spec.fnum() * *comm_spec.fnum();

  std::vector<byte_vector> vid_buffers(vertex_buffer_nums),
      vdata_buffers(vertex_buffer_nums), esrc_id_buffers(edge_buffer_nums),
      edst_id_buffers(edge_buffer_nums), edata_buffers(edge_buffer_nums);
  std::vector<offset_vector> vid_offsets(vertex_buffer_nums),
      esrc_id_offsets(edge_buffer_nums), edst_id_offsets(edge_buffer_nums);

  // fill in theses buffers in java
  callJavaLoader(vid_buffers, vid_offsets, vdata_buffers, esrc_id_buffers,
                 esrc_id_offsets, edst_id_buffers, edst_id_offsets,
                 edata_buffers);

  loader->AddVertexBuffers(vid_buffers, vid_offsets, vdata_buffers);
  VLOG(1) << "Finish add vertex buffers";
  loader->AddEdgeBuffers(esrc_id_buffers, esrc_id_offsets, edst_id_buffers,
                         edst_id_offsets, edata_buffers);
  VLOG(1) << "Finish add edge buffers";

  std::shared_ptr<_GRAPH_TYPE> fragment = loader->loadFragment();
  return fragment;
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
