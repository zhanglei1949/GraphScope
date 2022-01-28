
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

// consistent with vineyard::TypeToInt
template <size_t T>
struct IntToType {};

template <>
struct IntToType<2> {
  using TypeName = int32_t;
  using BuilderType =
      typename vineyard::ConvertToArrowType<TypeName>::BuilderType;
};
template <>
struct IntToType<4> {
  using TypeName = int64_t;
  using BuilderType =
      typename vineyard::ConvertToArrowType<TypeName>::BuilderType;
};

template <>
struct IntToType<6> {
  using TypeName = float;
  using BuilderType =
      typename vineyard::ConvertToArrowType<TypeName>::BuilderType;
};

template <>
struct IntToType<7> {
  using TypeName = double;
  using BuilderType =
      typename vineyard::ConvertToArrowType<TypeName>::BuilderType;
};

// indicates udf
template <>
struct IntToType<9> {
  using TypeName = std::string;
  using BuilderType = arrow::LargeStringBuilder;
};

static constexpr const char* JAVA_LOADER_CLASS =
    "com/alibaba/graphscope/loader/impl/FileLoader";
static constexpr const char* JAVA_LOADER_LOAD_VE_METHOD =
    "loadVerticesAndEdges";
static constexpr const char* JAVA_LOADER_LOAD_VE_SIG =
    "(Ljava/lang/String;Ljava/lang/String;)I";
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
static constexpr int GIRAPH_TYPE_CODE_LENGTH = 4;
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
    createFFIPointers();

    initJavaLoader();
    oid_type = vdata_type = edata_type = -1;
  }

  ~JavaLoaderInvoker() { VLOG(1) << "Destructing java loader invoker"; }

  void load_vertices_and_edges(const std::string& vertex_location) {
    size_t arg_pos = vertex_location.find_first_of('#');
    if (arg_pos != std::string::npos) {
      std::string file_path = vertex_location.substr(0, arg_pos);
      std::string json_params = vertex_location.substr(arg_pos + 1);
      VLOG(1) << "input path: " << file_path << " json params: " << json_params;

      int giraph_type_int =
          callJavaLoader(file_path.c_str(), json_params.c_str());
      CHECK(giraph_type_int >= 0);

      // fetch giraph graph types infos, so we can optimizing graph store by use
      // primitive types for LongWritable.
      parseGiraphTypeInt(giraph_type_int);

    } else {
      LOG(ERROR) << "No # found in vertex location";
    }
  }

  void load_edges(const std::string& edge_location) {
    LOG(ERROR) << "not implemented";
  }

  std::shared_ptr<arrow::Table> get_edge_table() {
    CHECK(oid_type > 0 && edata_type > 0);
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
            << " esrc len: [" << esrc_total_length << "] esrc total bytes: ["
            << esrc_total_bytes << "] edst len: [" << edst_total_length
            << "] esrc total bytes: [" << edst_total_bytes << "] edata len: ["
            << edata_total_length << "] edata total bytes: ["
            << edata_total_bytes << "]";

    double vertexTableBuildingTime = -grape::GetCurrentTime();

    std::shared_ptr<arrow::Array> esrc_array, edst_array, edata_array;

    buildArray<oid_type>(esrc_array, esrcs, esrc_offsets);
    buildArray<oid_type>(edst_array, edsts, edst_offsets);
    buildArray<edata_type>(edata_array, edatas, edata_offsets);
    VLOG(1) << "Finish edge array building";

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("src", arrow::large_utf8()),
                       arrow::field("dst", arrow::large_utf8()),
                       arrow::field("data", arrow::large_utf8())});

    auto res =
        arrow::Table::Make(schema, {esrc_array, edst_array, edata_array});
    VLOG(1) << "worker " << worker_id_
            << " generated table, rows:" << res->num_rows()
            << " cols: " << res->num_columns();

    edgeTableBuildingTime += grape::GetCurrentTime();
    VLOG(1) << "worker " << worker_id_
            << " Building vertex table cost: " << edgeTableBuildingTime;
    return res;
  }

  std::shared_ptr<arrow::Table> get_vertex_table() {
    CHECK(oid_type > 0 && vdata_type > 0);
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
            << " Building vertex table from oid array of size [" << oid_length
            << "] oid total bytes: [" << oid_total_bytes << "] vdata size: ["
            << vdata_total_length << "] total bytes: [" << vdata_total_bytes
            << "]";

    double vertexTableBuildingTime = -grape::GetCurrentTime();

    std::shared_ptr<arrow::Array> oid_array;
    std::shared_ptr<arrow::Array> vdata_array;

    buildArray<oid_type>(oid_array, oids, oid_offsets);
    buildArray<vdata_type>(vdata_array, vdatas, vdata_offsets);

    VLOG(1) << "Finish vertex array building";
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("oid", arrow::large_utf8()),
                       arrow::field("vdata", arrow::large_utf8())});

    auto res = arrow::Table::Make(schema, {oid_array, vdata_array});
    VLOG(1) << "worker " << worker_id_
            << " generated table, rows:" << res->num_rows()
            << " cols: " << res->num_columns();

    vertexTableBuildingTime += grape::GetCurrentTime();
    VLOG(1) << "worker " << worker_id_
            << " Building vertex table cost: " << vertexTableBuildingTime;
    return res;
  }

 private:
  template <size_t T>
  void buildArray(std::shared_ptr<arrow::Array>& array,
                  const std::vector<std::vector<char>>& data_arr,
                  const std::vector<std::vector<int>>& offset_arr) {
    VLOG(10) << "Building pod array with string builder";
    using elementType = typename IntToType<T>::TypeName;
    using builderType = typename IntToType<T>::BuilderType;
    builderType array_builder;
    int64_t total_length, total_bytes;
    for (auto i = 0; i < data_arr.size(); ++i) {
      total_bytes += data_arr[i].size();
      total_length += offset_arr[i].size();
    }
    array_builder.Reserve(total_length);  // the number of elements
    array_builder.ReserveData(total_bytes);

    for (auto i = 0; i < data_arr.size(); ++i) {
      auto ptr = static_cast<elementType*>(data_arr[i].data());
      auto cur_offset = offset_arr[i];

      for (size_t j = 0; j < cur_offset.size(); ++j) {
        // for appending data to arrow_binary_builder, we use raw pointer to
        // avoid copy.
        array_builder.UnsafeAppend(*ptr);
        CHECK(sizeof(*ptr) == cur_offset[j]);
        ptr += 1;  // We have convert to T*, so plus 1 is ok.
      }
    }
    array_builder.Finish(&array);
  }

  template <>
  void buildArray<9>(std::shared_ptr<arrow::Array>& array,
                     const std::vector<std::vector<char>>& data_arr,
                     const std::vector<std::vector<int>>& offset_arr) {
    VLOG(10) << "Building utf array with string builder";
    using elementType = typename IntToType<T>::TypeName;
    using builderType = typename IntToType<T>::BuilderType;
    builderType array_builder;
    int64_t total_length, total_bytes;
    for (auto i = 0; i < data_arr.size(); ++i) {
      total_bytes += data_arr[i].size();
      total_length += offset_arr[i].size();
    }
    array_builder.Reserve(total_length);  // the number of elements
    array_builder.ReserveData(total_bytes);

    for (auto i = 0; i < data_arr.size(); ++i) {
      const char* ptr = data_arr[i].data();
      auto cur_offset = offset_arr[i];

      for (size_t j = 0; j < cur_offset.size(); ++j) {
        // for appending data to arrow_binary_builder, we use raw pointer to
        // avoid copy.
        array_builder.UnsafeAppend(ptr, cur_offset[j]);
        ptr += cur_offset[j];  // We have convert to T*, so plus 1 is ok.
      }
    }
    array_builder.Finish(&array);
  }

  void createFFIPointers() {
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
  void initJavaLoader() {
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

  int callJavaLoader(const char* file_path, const char* java_params) {
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

      jint res = env->CallStaticIntMethod(
          loader_class, loader_method, file_path_jstring, java_params_jstring);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return;
      }

      javaLoadingTime += grape::GetCurrentTime();
      VLOG(1) << "Successfully Loaded graph data from Java loader, duration: "
              << javaLoadingTime;
      return res;
    } else {
      LOG(ERROR) << "Java env not available.";
      return -1;
    }
  }

  void parseGiraphTypeInt(const int giraph_type_int) {
    int edata_type = (giraph_type_int & 0x000F);
    giraph_type_int = giraph_type_int >> GIRAPH_TYPE_CODE_LENGTH);
    int vdata_type = (giraph_type_int & 0x000F);
    giraph_type_int = giraph_type_int >> GIRAPH_TYPE_CODE_LENGTH);
    int oid_type = (giraph_type_int & 0x000F);
    giraph_type_int = giraph_type_int >> GIRAPH_TYPE_CODE_LENGTH;
    CHECK(giraph_type_int == 0);
    VLOG(1) << "giraph types: " << oid_type << vdata_type << edata_type;
  }

  int worker_id_, worker_num_, load_thread_num;
  int oid_type, vdata_type, edata_type;
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
