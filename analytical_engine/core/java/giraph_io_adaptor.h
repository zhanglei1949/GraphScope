/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_CORE_IO_GIRAPH_JAVA_ADAPTOR_H_
#define ANALYTICAL_ENGINE_CORE_IO_GIRAPH_JAVA_ADAPTOR_H_

#include "jni.h"
#include "vineyard/io/io/local_io_adaptor.h"

namespace vineyard {

static const std::string DEFAULT_JAVA_LOADER_CLASS =
    "com.alibaba.graphscope.loader.impl.FileLoader";
static const std::string DEFAULT_JAVA_LOADER_METHOD_NAME =
    "com.alibaba.graphscope.loader.impl.FileLoader";
static const std::string DEFAULT_JAVA_LOADER_METHOD_SIG =
    "com.alibaba.graphscope.loader.impl.FileLoader";

class GiraphIOAdaptor : public LocalIOAdaptor {
 public:
  GiraphIOAdaptor(const std::string& location,
                  std::string giraphVertexReaderClass)
      : LocalIOAdaptor(location),
        giraphVertexReaderClass_(std::move(giraphVertexReaderClass)) {
    java_loader_invoker.Init(giraphVertexReaderClass_);
  }

  // This will not do the read, all read is done in constructor.
  Status ReadPartialTable(std::shared_ptr<arrow::Table>* table,
                          int index) override {
    VLOG(1) << "Read Partial table";
    // Read array from java
  }

 private:
  std::string giraphVertexReaderClass_;
  JavaLoaderInvoker java_loader_invoker;
};

class JavaLoaderInvoker {
 public:
  JavaLoaderInvoker(){};

  void Init(std::string&& giraphVertexReaderClass) {
    giraphVertexReaderClass_ = giraphVertexReaderClass_.c_str();
  }

  void ReadFromJava() {}

  jlong GetOidArrayBuilderAddress() {}

  jlong GetVdataArrayBuilderAddress() {}
  jlong GetEsrcArrayBuilderAddress() {}
  jlong GetEdstaArrayBuilderAddress() {}

  jlong GetEdataArrayBuilderAddress() {}

 private:
  const char* giraphVertexReaderClass_;
}
};  // namespace vineyard

#endif  // ANALYTICAL_ENGINE_CORE_IO_GIRAPH_JAVA_ADAPTOR_H_