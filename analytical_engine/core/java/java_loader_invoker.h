
#ifndef JAVA_LOADER_INVOKER_H
#define JAVA_LOADER_INVOKER_H

#include <vector>
#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "core/java/javasdk.h"
#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"

namespace gs {

static constexpr const char* JAVA_LOADER_CLASS =
    "com/alibaba/graphscope/loader/impl/FileLoader";
static constexpr const char* JAVA_LOADER_LOAD_VE_METHOD =
    "loadVerticesAndEdges";
static constexpr const char* JAVA_LOADER_LOAD_VE_SIG =
    "(Ljava/lang/String;Ljava/lang/String;)V";
static constexpr const char* JAVA_LOADER_INIT_METHOD = "init";
static constexpr const char* JAVA_LOADER_INIT_SIG =
    "(IIILcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;)V";
static constexpr const char* DATA_VECTOR_VECTOR =
    "std::vector<std::vector<char>>";
static constexpr const char* OFFSET_VECTOR_VECTOR =
    "std::vector<std::vector<int>>";
class JavaLoaderInvoker {
 public:
  JavaLoaderInvoker(int worker_id, int worker_num) {
    worker_id_ = worker_id;
    worker_num_ = worker_num;
    load_thread_num = 1;
    if (getenv("LOAD_THREAD_NUM")) {
      load_thread_num = atoi(getenv("USER_JAR_PATH"));
    }
    oids.resize(load_thread_num);
    vdatas.resize(load_thread_num);
    esrcs.resize(load_thread_num);
    edsts.resize(load_thread_num);
    edatas.resize(load_thread_num);

    oid_offsets.resize(load_thread_num);
    vdata_offsets.resize(load_thread_num);
    esrc_offsets.resize(load_thread_num);
    edst_offsets.resize(load_thread_num);
    edata_offsets.resize(load_thread_num);
    // Construct the FFIPointer
    create_FFIPointers();

    init_java_loader();
  }

  ~JavaLoaderInvoker() {}

  void load_vertices_and_edges(const std::string& vertex_location) {
    size_t arg_pos = vertex_location.find_first_of('#');
    if (arg_pos != std::string::npos) {
      std::string file_path = vertex_location.substr(0, arg_pos);
      std::string json_params = vertex_location.substr(arg_pos + 1);
      VLOG(1) << "input path: " << file_path << " json params: " << json_params;

      call_java_loader(file_path.c_str(), json_params.c_str());
    } else {
      LOG(ERROR) << "No # found in vertex location";
    }
  }

  void load_edges(const std::string& edge_location) {
    LOG(ERROR) << "not implemented";
  }

  std::shared_ptr<arrow::Table> get_edge_table() {
    // copy the data in std::vector<char> to arrowBinary builder.
    int64_t esrc_total_length = 0, edst_total_length = 0,
            edata_total_length = 0;
    int64_t esrc_total_bytes = 0, edst_total_bytes = 0, edata_total_bytes = 0;
    for (int i = 0; i < load_thread_num; ++i) {
      esrc_total_length += esrc_offsets[i].size();
      edst_total_length += edst_offsets[i].size();
      edata_total_length += edata_offsets[i].size();

      esrc_total_bytes += esrcs[i].size();
      edst_total_bytes += edsts[i].size();
      edata_total_bytes += edatas[i].size();
    }
    CHECK((esrc_total_length == edst_total_length) &&
          (edst_total_length == edata_total_length));
    VLOG(1) << "worker " << worker_id_ << " Building edge table "
            << " esrc len:" << esrc_total_length
            << " esrc total bytes: " << esrc_total_bytes
            << " edst len:" << edst_total_length
            << " esrc total bytes: " << edst_total_bytes
            << " edata len:" << edata_total_length
            << " edata total bytes: " << edata_total_length;
    arrow::StringBuilder esrc_array_builder, edst_array_builder,
        edata_array_builder;

    esrc_array_builder.Reserve(esrc_total_length);  // the number of elements
    esrc_array_builder.ReserveData(esrc_total_bytes);
    edst_array_builder.Reserve(edst_total_length);  // the number of elements
    edst_array_builder.ReserveData(edst_total_bytes);
    edata_array_builder.Reserve(edata_total_length);  // the number of elements
    edata_array_builder.ReserveData(edata_total_bytes);

    double edgeTableBuildingTime = -grape::GetCurrentTime();

    for (int i = 0; i < load_thread_num; ++i) {
      std::vector<char> cur_esrc_array = esrcs[i];
      std::vector<char> cur_edst_array = edsts[i];
      std::vector<char> cur_edata_array = edatas[i];

      std::vector<int> cur_esrc_offset = esrc_offsets[i];
      std::vector<int> cur_edst_offset = edst_offsets[i];
      std::vector<int> cur_edata_offset = edata_offsets[i];

      std::vector<char>::iterator esrc_iter = cur_esrc_array.begin();
      std::vector<char>::iterator edst_iter = cur_edst_array.begin();
      std::vector<char>::iterator edata_iter = cur_edata_array.begin();

      for (size_t j = 0; j < cur_esrc_offset.size(); ++j) {
        std::string tmp_esrc(esrc_iter, esrc_iter + cur_esrc_offset[j]);
        std::string tmp_edst(edst_iter, edst_iter + cur_edst_offset[j]);
        std::string tmp_edata(edata_iter, edata_iter + cur_edata_offset[j]);

        esrc_iter += cur_esrc_offset[j];
        edst_iter += cur_edst_offset[j];
        edata_iter += cur_edata_offset[j];
        VLOG(10) << "arrow array bulider, thread: " << i << " index: " << j
                 << " esrc : " << tmp_esrc.size()
                 << " edst: " << tmp_edst.size()
                 << " edata: " << tmp_edata.size();

        esrc_array_builder.UnsafeAppend(tmp_esrc);
        edst_array_builder.UnsafeAppend(tmp_edst);
        edata_array_builder.UnsafeAppend(tmp_edata);
      }
      VLOG(10) << "arrow array builder, finish build part: " << i;
    }

    std::shared_ptr<arrow::Array> esrc_array, edst_array, edata_array;
    esrc_array_builder.Finish(&esrc_array);
    edst_array_builder.Finish(&edst_array);
    edata_array_builder.Finish(&edata_array);

    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("src", arrow::utf8()), arrow::field("dst", arrow::utf8()),
         arrow::field("data", arrow::utf8())});

    auto res =
        arrow::Table::Make(schema, {esrc_array, edst_array, edata_array});
    VLOG(1) << "worker " << worker_id_
            << " generated table, rows:" << res->num_rows()
            << " cols: " << res->num_columns() << ": " << res->ToString();

    edgeTableBuildingTime += grape::GetCurrentTime();
    VLOG(1) << "worker " << worker_id_
            << " Building vertex table cost: " << edgeTableBuildingTime;
    return res;
  }

  std::shared_ptr<arrow::Table> get_vertex_table() {
    // copy the data in std::vector<char> to arrowBinary builder.
    int64_t oid_length = 0;
    int64_t oid_total_bytes = 0;
    int64_t vdata_total_length = 0;
    int64_t vdata_total_bytes = 0;
    for (int i = 0; i < load_thread_num; ++i) {
      oid_length += oid_offsets[i].size();
      vdata_total_length += vdata_offsets[i].size();
      oid_total_bytes += oids[i].size();
      vdata_total_bytes += vdatas[i].size();
    }
    CHECK(oid_length == vdata_total_length);
    VLOG(1) << "worker " << worker_id_
            << " Building vertex table from oid array of size " << oid_length
            << " oid total bytes: " << oid_total_bytes
            << " vdata size: " << vdata_total_length
            << " total bytes: " << vdata_total_bytes;
    arrow::StringBuilder oid_array_builder;
    arrow::StringBuilder vdata_array_builder;

    oid_array_builder.Reserve(oid_length);  // the number of elements
    oid_array_builder.ReserveData(oid_total_bytes);
    vdata_array_builder.Reserve(vdata_total_length);
    vdata_array_builder.ReserveData(vdata_total_bytes);

    double vertexTableBuildingTime = -grape::GetCurrentTime();

    for (int i = 0; i < load_thread_num; ++i) {
      std::vector<char> cur_oid_array = oids[i];
      std::vector<int> cur_oid_offset = oid_offsets[i];
      std::vector<char> cur_vdata_array = vdatas[i];
      std::vector<int> cur_vdata_offset = vdata_offsets[i];

      std::vector<char>::iterator oid_iter = cur_oid_array.begin();
      std::vector<char>::iterator vdata_iter = cur_vdata_array.begin();

      for (size_t j = 0; j < cur_oid_offset.size(); ++j) {
        std::string tmp_oid(oid_iter, oid_iter + cur_oid_offset[j]);
        std::string tmp_vdata(vdata_iter, vdata_iter + cur_vdata_offset[j]);
        oid_iter += cur_oid_offset[j];
        vdata_iter += cur_vdata_offset[j];
        VLOG(10) << "arrow array bulider, thread: " << i << " index: " << j
                 << " oidstr: " << tmp_oid.size()
                 << ", vdatastr: " << tmp_vdata.size();
        oid_array_builder.UnsafeAppend(tmp_oid);
        vdata_array_builder.UnsafeAppend(tmp_vdata);
      }
      VLOG(10) << "arrow array builder, finish build part: " << i;
    }

    std::shared_ptr<arrow::Array> oid_array;
    std::shared_ptr<arrow::Array> vdata_array;
    oid_array_builder.Finish(&oid_array);
    vdata_array_builder.Finish(&vdata_array);

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("oid", arrow::utf8()),
                       arrow::field("vdata", arrow::utf8())});

    auto res = arrow::Table::Make(schema, {oid_array, vdata_array});
    VLOG(1) << "worker " << worker_id_
            << " generated table, rows:" << res->num_rows()
            << " cols: " << res->num_columns() << ": " << res->ToString();

    vertexTableBuildingTime += grape::GetCurrentTime();
    VLOG(1) << "worker " << worker_id_
            << " Building vertex table cost: " << vertexTableBuildingTime;
    return res;
  }

 private:
  void create_FFIPointers() {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      std::string user_jar_path = getenv("USER_JAR_PATH");

      gs_class_loader_obj = gs::CreateClassLoader(env, user_jar_path);
      CHECK_NOTNULL(gs_class_loader_obj);
      {
        oids_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&oids));
        vdatas_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&vdatas));
        esrcs_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&esrcs));
        edsts_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edsts));
        edatas_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edatas));
      }
      {
        oid_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&oid_offsets));
        vdata_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&vdata_offsets));
        esrc_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&esrc_offsets));
        edst_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edst_offsets));
        edata_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edata_offsets));
      }
    }
    VLOG(1) << "Finish creating ffi wrappers";
  }
  // load the class and call init method
  void init_java_loader() {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);
      jmethodID loader_method = env->GetStaticMethodID(
          loader_class, JAVA_LOADER_INIT_METHOD, JAVA_LOADER_INIT_SIG);
      CHECK_NOTNULL(loader_method);

      env->CallStaticVoidMethod(
          loader_class, loader_method, worker_id_, worker_num_, load_thread_num,
          oids_jobj, vdatas_jobj, esrcs_jobj, edsts_jobj, edatas_jobj,
          oid_offsets_jobj, vdata_offsets_jobj, esrc_offsets_jobj,
          edst_offsets_jobj, edata_offsets_jobj);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Init java loader";
        return;
      }
    }
    VLOG(1) << "Successfully init java loader with params ";
  }

  void call_java_loader(const char* file_path, const char* java_params) {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);

      jmethodID loader_method = env->GetStaticMethodID(
          loader_class, JAVA_LOADER_LOAD_VE_METHOD, JAVA_LOADER_LOAD_VE_SIG);
      CHECK_NOTNULL(loader_method);

      jstring file_path_jstring = env->NewStringUTF(file_path);
      jstring java_params_jstring = env->NewStringUTF(java_params);
      double javaLoadingTime = -grape::GetCurrentTime();

      env->CallStaticVoidMethod(loader_class, loader_method, file_path_jstring,
                                java_params_jstring);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return;
      }

      javaLoadingTime += grape::GetCurrentTime();
      VLOG(1) << "Successfully Loaded graph data from Java loader, duration: "
              << javaLoadingTime;
    } else {
      LOG(ERROR) << "Java env not available.";
    }
  }

  int worker_id_, worker_num_, load_thread_num;
  std::vector<std::vector<char>> oids;
  std::vector<std::vector<char>> vdatas;
  std::vector<std::vector<char>> esrcs;
  std::vector<std::vector<char>> edsts;
  std::vector<std::vector<char>> edatas;

  std::vector<std::vector<int>> oid_offsets;
  std::vector<std::vector<int>> vdata_offsets;
  std::vector<std::vector<int>> esrc_offsets;
  std::vector<std::vector<int>> edst_offsets;
  std::vector<std::vector<int>> edata_offsets;

  jobject gs_class_loader_obj;

  jobject oids_jobj;
  jobject vdatas_jobj;
  jobject esrcs_jobj;
  jobject edsts_jobj;
  jobject edatas_jobj;

  jobject oid_offsets_jobj;
  jobject vdata_offsets_jobj;
  jobject esrc_offsets_jobj;
  jobject edst_offsets_jobj;
  jobject edata_offsets_jobj;

};  // namespace gs
};  // namespace gs

#endif  // JAVA_LOADER_INVOKER_H
