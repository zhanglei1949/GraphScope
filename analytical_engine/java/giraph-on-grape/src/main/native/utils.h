#ifndef UTILS_H
#define UTILS_H

#include <core/java/javasdk.h>
#include <jni.h>
#include <string>

#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

namespace grape {

// data vector contains all bytes, can be used to hold oid and vdata, edata.
using byte_vector = std::vector<char>;
// offset vector contains offsets to deserialize data vector.
using offset_vector = std::vector<int>;

static constexpr const char* OFFSET_VECTOR_VECTOR =
    "std::vector<std::vector<int>>";
static constexpr const char* DATA_VECTOR_VECTOR =
    "std::vector<std::vector<byte>>";

static constexpr const char* GIRAPH_PARAMS_CHECK_CLASS =
    "org/apache/giraph/utils/GiraphParamsChecker";
static constexpr const char* VERIFY_CLASSES_SIGN =
    "(Ljava/lang/String;Ljava/lang/String;)V";

static constexpr const char* OPTION_LOADING_THREAD_NUM = "loading_thread_num";
static constexpr const char* OPTION_INPUT_FORMAT_CLASS = "input_format_class";
static constexpr const char* OPTION_OUTPUT_FORMAT_CLASS = "output_format_class";
static const std::string DEFAULT_JAVA_LOADER_CLASS =
    "com.alibaba.graphscope.loader.impl.FileLoader";
static const std::string DEFAULT_JAVA_LOADER_METHOD_NAME =
    "com.alibaba.graphscope.loader.impl.FileLoader";
static const std::string DEFAULT_JAVA_LOADER_METHOD_SIG =
    "com.alibaba.graphscope.loader.impl.FileLoader";

using ptree = boost::property_tree::ptree;

void string2ptree(const std::string& params, ptree& pt) {
  std::stringstream ss;
  {
    ss << params;
    try {
      boost::property_tree::read_json(ss, pt);
    } catch (boost::property_tree::ptree_error& r) {
      LOG(ERROR) << "Parsing json failed: " << params;
    }
  }

  //   VLOG(2) << "Received json: " << params;
  //   std::string frag_name = pt.get<std::string>("frag_name");
}

void verifyClasses(const std::string& params) {
  // Before we proceed, check integrity of input params, then we go on to load
  // fragment.
  // init JVM, try to load classes and verify them
  JavaVM* jvm = gs::GetJavaVM();
  (void) jvm;
  CHECK_NOTNULL(jvm);
  VLOG(1) << "Successfully get jvm to verify classes";

  gs::JNIEnvMark m;
  if (m.env()) {
    JNIEnv* env = m.env();

    jclass param_checker_class = env->FindClass(GIRAPH_PARAMS_CHECK_CLASS);
    CHECK_NOTNULL(param_checker_class);

    jmethodID verify_class_method = env->GetStaticMethodID(
        param_checker_class, "verifyClasses", VERIFY_CLASSES_SIGN);
    CHECK_NOTNULL(verify_class_method);

    // jstring params_jstring = env->NewStringUTF(params.c_str());

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

template <typename T>
static T getFromPtree(const ptree& pt, const char* key) {
  T ret = pt.get<T>(key);
  if (ret) {
    return ret;
  } else {
    LOG(ERROR) << "No " << key << " in ptree:";
    return ret;
  }
}

}  // namespace grape

#endif  // UTILS_H
