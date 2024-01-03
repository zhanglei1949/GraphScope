
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
#include "flex/utils/property/column.h"

namespace gs {
template <typename EDATA_T>
class MMapVector {
 public:
  MMapVector(const std::string& work_dir, const std::string& file_name)
      : work_dir_(work_dir), file_name_(file_name) {
    array_.open(work_dir + "/" + file_name_ + ".tmp", false);
    array_.resize(1024);
    size_ = 0;
    cap_ = 1024;
  }
  ~MMapVector() {
    array_.reset();
    unlink((work_dir_ + "/" + file_name_ + ".tmp").c_str());
  }

  void push_back(const EDATA_T& val) {
    if (size_ == cap_) {
      array_.resize(cap_ * 2);
      cap_ = cap_ * 2;
    }
    array_.set(size_, val);
    ++size_;
  }

  void emplace_back(EDATA_T&& val) {
    if (size_ == cap_) {
      array_.resize(cap_ * 2);
      cap_ = cap_ * 2;
    }
    array_.set(size_, val);
    ++size_;
  }

  void resize(size_t size) {
    if (size > cap_) {
      array_.resize(size);
      cap_ = size;
    }
    size_ = size;
  }

  size_t size() const { return size_; }

  const EDATA_T& operator[](size_t index) const { return array_[index]; }
  EDATA_T& operator[](size_t index) { return array_[index]; }
  const EDATA_T* begin() const { return array_.data(); }
  const EDATA_T* end() const { return array_.data() + size_; }

  void clear() {
    size_ = 0;
    cap_ = 0;
  }

 private:
  mmap_array<EDATA_T> array_;
  std::string work_dir_;
  std::string file_name_;
  size_t size_;
  size_t cap_;
};
};      // namespace gs
#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_LOADER_UTILS_H_
