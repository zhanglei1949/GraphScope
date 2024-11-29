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

#ifndef RUNTIME_COMMON_GRAPH_INTERFACE_H_
#define RUNTIME_COMMON_GRAPH_INTERFACE_H_

#include "flex/engines/graph_db/runtime/common/schema.h"
#include "flex/engines/graph_db/runtime/common/types.h"

namespace gs {

namespace runtime {

#if 0

namespace graph_interface_impl {

using gs::label_t;
using gs::timestamp_t;
using gs::vid_t;

template <typename PROP_T>
class VertexColumn {
 public:
  VertexColumn(const std::shared_ptr<ColumnBase>& column) {
    if (column == nullptr) {
      column_ = nullptr;
    } else {
      column_ = dynamic_cast<const TypedColumn<PROP_T>*>(column.get());
    }
  }
  VertexColumn() : column_(nullptr) {}

  PROP_T get_view(vid_t v) const { return column_->get_view(v); }

  bool is_null() const { return column_ == nullptr; }

 private:
  const TypedColumn<PROP_T>* column_;
};

class VertexSet {
 public:
  VertexSet(vid_t size) : size_(size) {}
  ~VertexSet() {}

  class iterator {
   public:
    iterator(vid_t v) : v_(v) {}
    ~iterator() {}

    vid_t operator*() const { return v_; }

    iterator& operator++() {
      ++v_;
      return *this;
    }

    bool operator==(const iterator& rhs) const { return v_ == rhs.v_; }

    bool operator!=(const iterator& rhs) const { return v_ != rhs.v_; }

   private:
    vid_t v_;
  };

  iterator begin() const { return iterator(0); }
  iterator end() const { return iterator(size_); }
  size_t size() const { return size_; }

 private:
  vid_t size_;
};

class EdgeIterator {
 public:
  EdgeIterator(gs::ReadTransaction::edge_iterator&& iter)
      : iter_(std::move(iter)) {}
  ~EdgeIterator() {}

  Any GetData() const { return iter_.GetData(); }
  bool IsValid() const { return iter_.IsValid(); }
  void Next() { iter_.Next(); }
  vid_t GetNeighbor() const { return iter_.GetNeighbor(); }
  label_t GetNeighborLabel() const { return iter_.GetNeighborLabel(); }
  label_t GetEdgeLabel() const { return iter_.GetEdgeLabel(); }

 private:
  gs::ReadTransaction::edge_iterator iter_;
};

template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    // const_nbr_t provide two methods:
    // 1. vid_t get_neighbor() const;
    // 2. const EDATA_T& get_data() const;
    using const_nbr_t = typename gs::MutableNbrSlice<EDATA_T>::const_nbr_t;
    using const_nbr_ptr_t =
        typename gs::MutableNbrSlice<EDATA_T>::const_nbr_ptr_t;

   public:
    nbr_iterator(const_nbr_ptr_t ptr, const_nbr_ptr_t end,
                 timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_ != end_ && ptr_->get_timestamp() > timestamp_) {
        ++ptr_;
      }
    }

    const_nbr_t& operator*() const { return *ptr_; }

    const_nbr_ptr_t operator->() const { return ptr_; }

    nbr_iterator& operator++() {
      ++ptr_;
      while (ptr_ != end_ && ptr_->get_timestamp() > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      return (ptr_ == rhs.ptr_);
    }

    bool operator!=(const nbr_iterator& rhs) const {
      return (ptr_ != rhs.ptr_);
    }

   private:
    const_nbr_ptr_t ptr_;
    const_nbr_ptr_t end_;
    timestamp_t timestamp_;
  };

 public:
  using slice_t = gs::MutableNbrSlice<EDATA_T>;
  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView() : csr_(nullptr), timestamp_(0), unsorted_since_(0) {}
  GraphView(const gs::MutableCsr<EDATA_T>* csr, timestamp_t timestamp)
      : csr_(csr),
        timestamp_(timestamp),
        unsorted_since_(csr->unsorted_since()) {}

  bool is_null() const { return csr_ == nullptr; }

  AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_->get_edges(v), timestamp_);
  }

  /*
   * void func(vid_t v, const EDATA_T& data) {
   *     // do something
   * }
   */
  template <typename FUNC_T>
  void foreach_edges_gt(vid_t v, const EDATA_T& min_value,
                        const FUNC_T& func) const {
    const auto& edges = csr_->get_edges(v);
    auto ptr = edges.end() - 1;
    auto end = edges.begin() - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp_) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since_) {
        break;
      }
      if (min_value < ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    while (ptr != end) {
      if (ptr->data < min_value) {
        break;
      }
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

  template <typename FUNC_T>
  void foreach_edges_lt(vid_t v, const EDATA_T& max_value,
                        const FUNC_T& func) const {
    const auto& edges = csr_->get_edges(v);
    auto ptr = edges.end() - 1;
    auto end = edges.begin() - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp_) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since_) {
        break;
      }
      if (ptr->data < max_value) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    if (ptr == end) {
      return;
    }
    ptr = std::upper_bound(end + 1, ptr + 1, max_value,
                           [](const EDATA_T& a, const MutableNbr<EDATA_T>& b) {
                             return a < b.data;
                           }) -
          1;
    while (ptr != end) {
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

 private:
  const gs::MutableCsr<EDATA_T>* csr_;
  timestamp_t timestamp_;
  timestamp_t unsorted_since_;
};

template <typename T>
class VertexArray {
 public:
  VertexArray() : data_() {}
  VertexArray(const VertexSet& keys, const T& val) : data_(keys.size(), val) {}
  ~VertexArray() {}

  void Init(const VertexSet& keys, const T& val) {
    data_.resize(keys.size(), val);
  }

  typename std::vector<T>::reference operator[](vid_t v) { return data_[v]; }
  typename std::vector<T>::const_reference operator[](vid_t v) const {
    return data_[v];
  }

 private:
  std::vector<T> data_;
};

}  // namespace graph_interface_impl

class GraphReadInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = graph_interface_impl::VertexColumn<PROP_T>;

  using vertex_set_t = graph_interface_impl::VertexSet;

  using edge_iterator_t = graph_interface_impl::EdgeIterator;

  template <typename EDATA_T>
  using graph_view_t = graph_interface_impl::GraphView<EDATA_T>;

  template <typename T>
  using vertex_array_t = graph_interface_impl::VertexArray<T>;

  static constexpr vid_t kInvalidVid = std::numeric_limits<vid_t>::max();

  GraphReadInterface(const gs::ReadTransaction& txn) : txn_(txn) {}
  ~GraphReadInterface() {}

  template <typename PROP_T>
  vertex_column_t<PROP_T> GetVertexColumn(label_t label,
                                          const std::string& prop_name) const {
    return vertex_column_t<PROP_T>(
        txn_.get_vertex_property_column(label, prop_name));
  }

  vertex_set_t GetVertexSet(label_t label) const {
    return vertex_set_t(txn_.GetVertexNum(label));
  }

  bool GetVertexIndex(label_t label, const Any& id, vid_t& index) const {
    return txn_.GetVertexIndex(label, id, index);
  }

  Any GetVertexId(label_t label, vid_t index) const {
    return txn_.GetVertexId(label, index);
  }

  Any GetVertexProperty(label_t label, vid_t index, int prop_id) const {
    return txn_.graph().get_vertex_table(label).at(index, prop_id);
  }

  edge_iterator_t GetOutEdgeIterator(label_t label, vid_t v,
                                     label_t neighbor_label,
                                     label_t edge_label) const {
    return edge_iterator_t(
        txn_.GetOutEdgeIterator(label, v, neighbor_label, edge_label));
  }

  edge_iterator_t GetInEdgeIterator(label_t label, vid_t v,
                                    label_t neighbor_label,
                                    label_t edge_label) const {
    return edge_iterator_t(
        txn_.GetInEdgeIterator(label, v, neighbor_label, edge_label));
  }

  template <typename EDATA_T>
  graph_view_t<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                             label_t neighbor_label,
                                             label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        txn_.graph().get_oe_csr(v_label, neighbor_label, edge_label));
    return graph_view_t<EDATA_T>(csr, txn_.timestamp());
  }

  template <typename EDATA_T>
  graph_view_t<EDATA_T> GetIncomingGraphView(label_t v_label,
                                             label_t neighbor_label,
                                             label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        txn_.graph().get_ie_csr(v_label, neighbor_label, edge_label));
    return graph_view_t<EDATA_T>(csr, txn_.timestamp());
  }

  const Schema& schema() const { return txn_.schema(); }

 private:
  const gs::ReadTransaction& txn_;
};

#else

// A dummy graph class to make the code compile

namespace graph_interface_impl {
class DummyGraph {
 public:
  DummyGraph() {}
  ~DummyGraph() {}

  const Schema& schema() const { return schema_; }

 private:
  Schema schema_;
};

template <typename PROP_T>
class VertexColumn {
 public:
  VertexColumn(const std::shared_ptr<ColumnBase>& column) {}
  VertexColumn() {}

  PROP_T get_view(vid_t v) const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return PROP_T();
  }

  bool is_null() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return false;
  }
};

class VertexSet {
 public:
  VertexSet(vid_t size) {}
  ~VertexSet() {}

  class iterator {
   public:
    iterator(vid_t v) {}
    ~iterator() {}

    vid_t operator*() const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return 0;
    }

    iterator& operator++() {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return *this;
    }

    bool operator==(const iterator& rhs) const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return false;
    }

    bool operator!=(const iterator& rhs) const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return false;
    }
  };

  iterator begin() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return iterator(0);
  }
  iterator end() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return iterator(0);
  }
  size_t size() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return 0;
  }
};
class EdgeIterator {
 public:
  EdgeIterator() {}
  ~EdgeIterator() {}

  Any GetData() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Any();
  }
  bool IsValid() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return false;
  }
  void Next() { LOG(FATAL) << "NOT IMPLEMENTED"; }
  vid_t GetNeighbor() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return 0;
  }
  label_t GetNeighborLabel() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return 0;
  }
  label_t GetEdgeLabel() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return 0;
  }
};

template <typename EDATA_T>
class Nbr {
 public:
  Nbr() {}
  ~Nbr() {}

  vid_t get_neighbor() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return 0;
  }

  EDATA_T get_data() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return EDATA_T();
  }
};

template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    // const_nbr_t provide two methods:
    // 1. vid_t get_neighbor() const;
    // 2. const EDATA_T& get_data() const;
    // using const_nbr_t = typename gs::MutableNbrSlice<EDATA_T>::const_nbr_t;
    // using const_nbr_ptr_t =
    //     typename gs::MutableNbrSlice<EDATA_T>::const_nbr_ptr_t;
    using const_nbr_t = const Nbr<EDATA_T>;
    using const_nbr_ptr_t = const Nbr<EDATA_T>*;

   public:
    nbr_iterator() {}

    const_nbr_t& operator*() const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return dummy;
    }

    const_nbr_ptr_t operator->() const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return nullptr;
    }

    nbr_iterator& operator++() {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return false;
    }

    bool operator!=(const nbr_iterator& rhs) const {
      LOG(FATAL) << "NOT IMPLEMENTED";
      return false;
    }

   private:
    Nbr<EDATA_T> dummy;
  };

 public:
  // using slice_t = gs::MutableNbrSlice<EDATA_T>;
  AdjListView() {}

  nbr_iterator begin() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return nbr_iterator();
  }
  nbr_iterator end() const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return nbr_iterator();
  }
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView() {}

  bool is_null() const { return false; }

  AdjListView<EDATA_T> get_edges(vid_t v) const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return AdjListView<EDATA_T>();
  }

  /*
   * void func(vid_t v, const EDATA_T& data) {
   *     // do something
   * }
   */
  template <typename FUNC_T>
  void foreach_edges_gt(vid_t v, const EDATA_T& min_value,
                        const FUNC_T& func) const {
    LOG(FATAL) << "NOT IMPLEMENTED";
  }

  template <typename FUNC_T>
  void foreach_edges_lt(vid_t v, const EDATA_T& max_value,
                        const FUNC_T& func) const {
    LOG(FATAL) << "NOT IMPLEMENTED";
  }
};

template <typename T>
class VertexArray {
 public:
  VertexArray() {}
  VertexArray(const VertexSet& keys, const T& val) {}
  ~VertexArray() {}

  void Init(const VertexSet& keys, const T& val) {
    LOG(FATAL) << "NOT IMPLEMENTED";
  }

  typename std::vector<T>::reference operator[](vid_t v) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return data_[v];
  }
  typename std::vector<T>::const_reference operator[](vid_t v) const {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return data_[v];
  }

 private:
  std::vector<T> data_;
};
}  // namespace graph_interface_impl

class GraphReadInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = typename graph_interface_impl::VertexColumn<PROP_T>;

  using vertex_set_t = typename graph_interface_impl::VertexSet;

  using edge_iterator_t = typename graph_interface_impl::EdgeIterator;

  template <typename EDATA_T>
  using graph_view_t = typename graph_interface_impl::GraphView<EDATA_T>;

  template <typename T>
  using vertex_array_t = typename graph_interface_impl::VertexArray<T>;

  static constexpr vid_t kInvalidVid = std::numeric_limits<vid_t>::max();

  GraphReadInterface(const graph_interface_impl::DummyGraph& graph)
      : graph_(graph) {}
  ~GraphReadInterface() {}

  template <typename PROP_T>
  vertex_column_t<PROP_T> GetVertexColumn(label_t label,
                                          const std::string& prop_name) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return vertex_column_t<PROP_T>();
  }

  vertex_set_t GetVertexSet(label_t label) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return vertex_set_t(0);
  }

  bool GetVertexIndex(label_t label, const Any& id, vid_t& index) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return false;
  }

  Any GetVertexId(label_t label, vid_t index) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return Any();
  }

  Any GetVertexProperty(label_t label, vid_t index, int prop_id) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return Any();
  }

  edge_iterator_t GetOutEdgeIterator(label_t label, vid_t v,
                                     label_t neighbor_label,
                                     label_t edge_label) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return edge_iterator_t();
  }

  edge_iterator_t GetInEdgeIterator(label_t label, vid_t v,
                                    label_t neighbor_label,
                                    label_t edge_label) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return edge_iterator_t();
  }

  template <typename EDATA_T>
  graph_view_t<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                             label_t neighbor_label,
                                             label_t edge_label) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return graph_view_t<EDATA_T>();
  }

  template <typename EDATA_T>
  graph_view_t<EDATA_T> GetIncomingGraphView(label_t v_label,
                                             label_t neighbor_label,
                                             label_t edge_label) const {
    LOG(INFO) << "NOT IMPLEMENTED";
    return graph_view_t<EDATA_T>();
  }

  const Schema& schema() const { return graph_.schema(); }

 private:
  const graph_interface_impl::DummyGraph& graph_;
};

#endif

// class GraphInsertInterface {
//  public:
//   GraphInsertInterface(gs::InsertTransaction& txn) : txn_(txn) {}
//   ~GraphInsertInterface() {}

//   bool AddVertex(label_t label, const Any& id, const std::vector<Any>& props)
//   {
//     return txn_.AddVertex(label, id, props);
//   }

//   bool AddEdge(label_t src_label, const Any& src, label_t dst_label,
//                const Any& dst, label_t edge_label, const Any& prop) {
//     return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, prop);
//   }

//   void Commit() { txn_.Commit(); }

//   void Abort() { txn_.Abort(); }

//   const Schema& schema() const { return txn_.schema(); }

//  private:
//   gs::InsertTransaction& txn_;
// };

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_GRAPH_INTERFACE_H_