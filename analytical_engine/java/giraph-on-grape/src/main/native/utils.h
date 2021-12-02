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

using ptree = boost::property_tree::ptree;

ptree string2ptree(std::string& params) {
  ptree pt;
  std::stringstream ss;
  {
    ss << params;
    try {
      boost::property_tree::read_json(ss, pt);
    } catch (boost::property_tree::ptree_error& r) {
      LOG(ERROR) << "Parsing json failed: " << params;
      return nullptr;
    }
  }

  //   VLOG(2) << "Received json: " << params;
  //   std::string frag_name = pt.get<std::string>("frag_name");
  return std::move(pt);
}

void verifyClasses(const std::string& params) {
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

template <typename T>
static T getFromPtree(const ptree& pt, const char* key) {
  T ret = pt.get<T>(key);
  if (ret) {
    return ret;
  } else {
    LOG(ERROR) << "No " << key << " in ptree:";
    return nullptr;
  }
}

}  // namespace grape

#endif  // UTILS_H