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

#ifndef GRAPE_FRAGMENT_GIRAPH_FRAGMENT_LOADER_H_
#define GRAPE_FRAGMENT_GIRAPH_FRAGMENT_LOADER_H_

#include <mpi.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grape/fragment/basic_fragment_loader.h"
#include "grape/fragment/partitioner.h"
#include "grape/io/line_parser_base.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/io/tsv_line_parser.h"
#include "grape/worker/comm_spec.h"

namespace grape {

/**
 * @brief Giraph fragment loader loads vertex and edges for giraph app.
 *
 * @tparam FRAG_T Fragment type.
 */
template <typename FRAG_T>
class GiraphFragmentLoader {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;

  using vertex_map_t = typename fragment_t::vertex_map_t;

  static constexpr LoadStrategy load_strategy = fragment_t::load_strategy;

 public:
  explicit GiraphFragmentLoader(const CommSpec comm_spec)
      : comm_spec_(comm_spec) {}

  ~GiraphFragmentLoader() = default;

  void AddVertexBuffers(std::vector<std::vector<char>>&& vidBuffers,
                        std::vector<std::vector<int>>&& vidOffsets,
                        std::vector<std::vector<char>>&& vdataBuffers) {}

  void AddEdgeBuffers(std::vector<std::vector<char>>&& esrcBuffers,
                        std::vector<std::vector<int>>&& esrcOffsets,
                        std::vector<std::vector<char>>&& edstBuffers,
                        std::vector<std::vector<int>>&& edstOffsets,
                        std::vector<std::vector<char>>&& edataBuffers) {}

  std::shared_ptr<fragment_t> LoadFragment() {
    std::shared_ptr<fragment_t> fragment(nullptr);

    return fragment;
  }

 private:
  CommSpec comm_spec_;
};

}  // namespace grape

#endif  // GRAPE_FRAGMENT_EV_FRAGMENT_LOADER_H_
