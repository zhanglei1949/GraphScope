/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>

#include <iomanip>
#include <limits>
#include <string>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

// Don't include java context base
#include "grape/app/context_base.h"
#include "grape/grape.h"

#include "core/error.h"
#include "core/java/javasdk.h"

namespace gs {
static constexpr const char* JSON_CLASS_NAME = "com.alibaba.fastjson.JSON";
static constexpr const char* APP_CONTEXT_GETTER_CLASS =
    "com/alibaba/graphscope/utils/AppContextGetter";
static constexpr const char* LOAD_LIBRARY_CLASS =
    "com/alibaba/graphscope/utils/LoadLibrary";
/**
 * @brief Driver context for Java context, work along with @see
 * gs::JavaPIEDefaultApp.
 *
 * @tparam FRAG_T Should be grape::ImmutableEdgecutFragment<...>
 */
template <typename FRAG_T>
class JavaPIEDefaultContext : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;
  using ptree = boost::property_tree::ptree;
  explicit JavaPIEDefaultContext(const FRAG_T& fragment)
      : fragment_(fragment),
        app_class_name_(NULL),
        context_class_name_(NULL),
        app_object_(NULL),
        context_object_(NULL),
        fragment_object_impl_(NULL),
        fragment_object_(NULL),
        mm_object_(NULL),
        url_class_loader_object_(NULL) {}

  virtual ~JavaPIEDefaultContext() {
    delete[] app_class_name_;
    delete[] context_class_name_;
    JNIEnvMark m;
    if (m.env()) {
      m.env()->DeleteGlobalRef(app_object_);
      m.env()->DeleteGlobalRef(context_object_);
      m.env()->DeleteGlobalRef(fragment_object_impl_);
      m.env()->DeleteGlobalRef(fragment_object_);
      m.env()->DeleteGlobalRef(mm_object_);
      m.env()->DeleteGlobalRef(url_class_loader_object_);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }
  const fragment_t& fragment() { return fragment_; }
  const char* app_class_name() const { return app_class_name_; }
  const char* context_class_name() const { return context_class_name_; }
  const jobject& app_object() const { return app_object_; }
  const jobject& context_object() const { return context_object_; }
  const jobject& fragment_object() const { return fragment_object_; }
  const jobject& message_manager_object() const { return mm_object_; }
  const jobject& url_class_loader_object() const {
    return url_class_loader_object_;
  }

  void Init(grape::DefaultMessageManager& messages,
            const std::string& app_class_name, const std::string& frag_name,
            const std::string& user_lib_path, const std::string& params) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      {
        // Create a gs class loader obj which has same classPath with parent
        // classLoader.
        std::string gs_classLoader_cp;
        if (getenv("USER_JAR_PATH")) {
          gs_classLoader_cp = getenv("USER_JAR_PATH");
        }
	VLOG(1) << "Created class loader with cp: " << gs_classLoader_cp;
        jobject gs_class_loader_obj = CreateClassLoader(env, gs_classLoader_cp);
        CHECK_NOTNULL(gs_class_loader_obj);
        url_class_loader_object_ = env->NewGlobalRef(gs_class_loader_obj);
      }
      LoadUserLibrary(user_lib_path);

      CHECK(!app_class_name.empty());
      app_class_name_ = JavaClassNameDashToSlash(app_class_name);
      app_object_ =
          LoadAndCreate(env, url_class_loader_object_, app_class_name_);
      std::string app_context_name = getContextNameFromAppClass(env);
      context_class_name_ = JavaClassNameDashToSlash(app_context_name);
      // CHECK_NOTNULL(!initClassNames(app_class_name, app_context_name));

      context_object_ =
          LoadAndCreate(env, url_class_loader_object_, context_class_name_);

      java_frag_type_name_ = frag_name;
      fragment_object_impl_ = CreateFFIPointer(
          env, java_frag_type_name_.c_str(), url_class_loader_object_,
          reinterpret_cast<jlong>(&fragment_));
      // wrap immutable or projected fragment as simple fragment
      fragment_object_ = ImmutableFragment2Simple(env, url_class_loader_object_,
                                                  fragment_object_impl_);
      mm_object_ = CreateFFIPointer(env, default_java_message_mananger_name_,
                                    url_class_loader_object_,
                                    reinterpret_cast<jlong>(&messages));

      // jobject args_object = CreateFFIPointer(env, "std::vector<std::string>",
      //                                        url_class_loader_object_,
      //                                        reinterpret_cast<jlong>(&args));

      jclass json_class = (jclass) LoadClassWithClassLoader(
          env, url_class_loader_object_, JSON_CLASS_NAME);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in loading json class ";
      }
      CHECK_NOTNULL(json_class);
      jmethodID parse_method = env->GetStaticMethodID(
          json_class, "parseObject",
          "(Ljava/lang/String;)Lcom/alibaba/fastjson/JSONObject;");
      CHECK_NOTNULL(parse_method);
      VLOG(1) << "User defined kw args: " << params;
      jstring args_jstring = env->NewStringUTF(params.c_str());
      jobject json_object =
          env->CallStaticObjectMethod(json_class, parse_method, args_jstring);
      CHECK_NOTNULL(json_object);
      const char* descriptor =
          "(Lcom/alibaba/graphscope/fragment/SimpleFragment;"
          "Lcom/alibaba/graphscope/parallel/DefaultMessageManager;"
          "Lcom/alibaba/fastjson/JSONObject;)V";

      jclass context_class = env->FindClass(context_class_name_);
      CHECK_NOTNULL(context_class);
      jmethodID init_methodID =
          env->GetMethodID(context_class, "Init", descriptor);
      CHECK_NOTNULL(init_methodID);

      env->CallVoidMethod(context_object_, init_methodID, fragment_object_,
                          mm_object_, json_object);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

  void Output(std::ostream& os) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jclass context_class = env->FindClass(context_class_name_);
      CHECK_NOTNULL(context_class);

      const char* descriptor =
          "(Lcom/alibaba/graphscope/fragment/SimpleFragment;)V";
      jmethodID output_methodID =
          env->GetMethodID(context_class, "Output", descriptor);
      CHECK_NOTNULL(output_methodID);
      env->CallVoidMethod(context_object_, output_methodID, fragment_object_);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

 private:
  // get the java context name with is bounded to app_object_.
  std::string getContextNameFromAppClass(JNIEnv* env) {
    jclass app_context_getter_class = (jclass) LoadClassWithClassLoader(
        env, url_class_loader_object_, APP_CONTEXT_GETTER_CLASS);
    if (env->ExceptionCheck()) {
      LOG(ERROR) << "Exception in loading class: "
                 << std::string(APP_CONTEXT_GETTER_CLASS);
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(ERROR) << "exiting since exception occurred";
    }

    CHECK_NOTNULL(app_context_getter_class);

    jmethodID app_context_getter_method =
        env->GetStaticMethodID(app_context_getter_class, "getContextName",
                               "(Ljava/lang/Object;)Ljava/lang/String;");
    CHECK_NOTNULL(app_context_getter_method);
    // Pass app class's class object
    jstring context_class_jstring = (jstring) env->CallStaticObjectMethod(
        app_context_getter_class, app_context_getter_method, app_object_);
    CHECK_NOTNULL(context_class_jstring);
    return JString2String(env, context_class_jstring);
  }
  void LoadUserLibrary(const std::string& user_library_name) {
    // Before query make sure the jni lib is loaded
    if (!user_library_name.empty()) {
      // Since we load loadLibraryClass with urlClassLoader, the
      // fromClass.classLoader literal, which is used in System.load, should
      // be urlClassLoader.
      jclass load_library_class = (jclass) LoadClassWithClassLoader(
          env, url_class_loader_object_, LOAD_LIBRARY_CLASS);
      CHECK_NOTNULL(load_library_class);
      jstring user_library_jstring =
          env->NewStringUTF(user_library_name.c_str());
      jmethodID load_library_methodID = env->GetStaticMethodID(
          load_library_class, "invoke", "(Ljava/lang/String;)V");

      // call static method
      env->CallStaticVoidMethod(load_library_class, load_library_methodID,
                                user_library_jstring);
      if (env->ExceptionCheck()) {
        LOG(ERROR) << "Exception occurred when loading user library";
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exiting since exception occurred";
      }
      VLOG(1) << "Loaded specified user jni library: " << user_library_name;
    } else {
      LOG(1) << "Skipping loadin jni lib since user_library_name none";
    }
  }

  const fragment_t& fragment_;
  char* app_class_name_;
  char* context_class_name_;
  static constexpr char* default_java_message_mananger_name_ =
      "grape::DefaultMessageManager";
  std::string java_frag_type_name_;
  jobject app_object_;
  jobject context_object_;
  jobject fragment_object_impl_;
  jobject fragment_object_;
  jobject mm_object_;
  jobject url_class_loader_object_;
};  // namespace gs
}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
