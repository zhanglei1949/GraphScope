#ifndef JAVA_LOADER_INVOKER_H
#define JAVA_LOADER_INVOKER_H

#include <jni.h>
#include <string>

#include <glog/logging.h>

#include "core/java/javasdk.h"
#include "utils.h"

namespace grape {
class JavaLoaderInvoker {
 public:
  explicit JavaLoaderInvoker()
      : vertex_input_format_class_(nullptr), java_loader_class_(nullptr), loader_method_name_(nullptr), loader_method_sig_(nullptr) {};

  ~JavaLoaderInvoker() {
    if (vertex_input_format_class_) {
      delete[] vertex_input_format_class_;
    }
    if (java_loader_class_) {
      delete[] java_loader_class_;
    }
  }

  /**
   * @brief Call java loader to fillin buffers
   *
   */
  void CallJavaLoader(std::vector<byte_vector>& vid_buffers,
                      std::vector<offset_vector>& vid_offsets,
                      std::vector<byte_vector>& vdata_buffers,
                      std::vector<byte_vector>& esrc_id_buffers,
                      std::vector<offset_vector>& esrc_id_offsets,
                      std::vector<byte_vector>& edst_id_buffers,
                      std::vector<offset_vector>& edst_id_offsets,
                      std::vector<byte_vector>& edata_buffers) {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      // create ffi pointers.

      jobject gs_class_loader_obj = gs::CreateClassLoader(env);
      CHECK_NOTNULL(gs_class_loader_obj);

      jobject vid_buffers_jobj = gs::CreateFFIPointer(
          env, DATA_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&vid_buffers));
      jobject vid_offsets_jobj = gs::CreateFFIPointer(
          env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&vid_offsets));
      jobject vdata_buffers_jobj = gs::CreateFFIPointer(
          env, DATA_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&vdata_buffers));
      jobject esrc_id_buffers_jobj = gs::CreateFFIPointer(
          env, DATA_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&esrc_id_buffers));
      jobject esrc_id_offsets_jobj = gs::CreateFFIPointer(
          env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&esrc_id_offsets));
      jobject edst_id_buffers_jobj = gs::CreateFFIPointer(
          env, DATA_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&edst_id_buffers));
      jobject edst_id_offsets_jobj = gs::CreateFFIPointer(
          env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&edst_id_offsets));
      jobject edata_buffers_jobj = gs::CreateFFIPointer(
          env, DATA_VECTOR_VECTOR, gs_class_loader_obj, reinterpret_cast<jlong>(&edata_buffers));

      VLOG(1) << "Finish creating ffi wrappers";

      jclass loader_class = env->FindClass(java_loader_class_);
      CHECK_NOTNULL(loader_class);
      jmethodID loader_method =
          env->GetStaticMethodID(loader_class, loader_method_name_, loader_method_sig_);
      CHECK_NOTNULL(loader_method);
      double javaLoadingTime = -GetCurrentTime();

      env->CallStaticVoidMethod(
          loader_class, loader_method, vid_buffers_jobj, vid_offsets_jobj,
          vdata_buffers_jobj, esrc_id_buffers_jobj, esrc_id_offsets_jobj,
          edst_id_buffers_jobj, edst_id_offsets_jobj, edata_buffers_jobj);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return;
      }

      javaLoadingTime += GetCurrentTime();
      VLOG(1) << "Successfully Loaded graph data from Java loader, duration:"
              << javaLoadingTime;
    } else {
      LOG(ERROR) << "Java env not available.";
    }
  }

  void Init(const std::string& java_loader_class,
            const std::string loader_method_name,
            const std::string loader_method_sig,
            const std::string& vertex_input_format_class) {
    java_loader_class_ = gs::JavaClassNameDashToSlash(java_loader_class);
    if (vertex_input_format_class.empty()){
        LOG(ERROR) << "Empty vertex input class string";
        return ;
    }
    vertex_input_format_class_ = vertex_input_format_class.c_str();
    loader_method_name_ = loader_method_name.c_str();
    loader_method_sig_ = loader_method_sig.c_str();
  }

 private:
  const char* vertex_input_format_class_;
  const char* java_loader_class_;
  const char* loader_method_name_;
  const char* loader_method_sig_;
};
}  // namespace grape

#endif  // JAVA_LOADER_INVOKER_H
