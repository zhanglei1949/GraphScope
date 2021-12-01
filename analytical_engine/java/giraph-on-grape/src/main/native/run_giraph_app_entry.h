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

#define QUOTE(X) #X

#if !defined(_GRAPH_TYPE)
#error "Missing _GRAPH_TYPE"
#endif

#if !defined(_APP_TYPE)
#error "Missing macro _APP_TYPE"
#endif

using oid_t = _GRAPH_TYPE::oid_t;
using vdata_t = _GRAPH_TYPE::vdata_t;
using edata_t = _GRAPH_TYPE::edata_t;
using vid_t = uint64_t;

namespace gs {

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

  // Before we proceed, check integrity of input params, then we go on to load
  // fragment.
  // init JVM, try to load classes and verify them
  JavaVM* jvm = GetJavaVM();
  (void) jvm;
  CHECK_NOTNULL(jvm);
  VLOG(1) << "Successfully get jvm to verify classes";

  JNIEnvMark m;
  if (m.env()) {
    JNIEnv* env = m.env();

    jclass param_checker_class = env->FindClass(GIRAPH_PARAMS_CHECK_CLASS);
    CHECK_NOTNULL(param_checker_class);

    jmethodID verify_class_method = env->GetStaticMethodID(
        param_checker_class, "verifyClasses", VERIFY_CLASSES_SIGN);
    CHECK_NOTNULL(verify_class_method);

    jstring params_jstring = env->NewStringUTF(params.c_str());

    // env->CallStaticVoidMethod(param_checker_class, verify_class_method,
    //                           params_jstring);
    if (env->ExceptionCheck()) {
      LOG(ERROR) << "Exception occurred when verify classes";
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(ERROR) << "Exiting since exception occurred";
    }
    VLOG(2) << "Params verified: " << params;
  }
}

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

//  ImmutableEdgecutFragmentLoader<oid_t, vid_t, vdata_t, edata_t> loader(
//      comm_spec);
//  std::shared_ptr<_GRAPH_TYPE> fragment =
//      loader.loadFragment(params.vfile, params.efile, directed);

  // Query<_GRAPH_TYPE>(comm_spec, fragment, params);
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
