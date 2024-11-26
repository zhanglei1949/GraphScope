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

#ifndef RUNTIME_COMMON_COLUMNS_VERTEX_PROPERTY_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_VERTEX_PROPERTY_COLUMNS_H_

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"

namespace gs {

namespace runtime {

// class IVertexPropertyColumn : public IContextColumn {
//  public:
//   IVertexPropertyColumn() = default;
//   virtual ~IVertexPropertyColumn() = default;
//
//   virtual const std::shared_ptr<IVertexColumn> get_vertex_column() const = 0;
//   virtual void set_vertex_column(
//       const std::shared_ptr<IVertexColumn>& vertices) = 0;
// };

template <typename T>
class SLVertexPropertyColumn : public IValueColumn<T> {
 public:
  SLVertexPropertyColumn() = default;
  SLVertexPropertyColumn(const GraphReadInterface& graph,
                         std::shared_ptr<SLVertexColumn> vertices,
                         const std::string& prop_name) {
    vertices_col_ = vertices;
    vertices_ = vertices_col_.get();
    prop_col_ = graph.GetVertexColumn<T>(vertices_->label(), prop_name);
  }
  ~SLVertexPropertyColumn() = default;

  const std::shared_ptr<IContextColumn> get_vertex_column() const override {
    return vertices_col_;
  }

  std::shared_ptr<IContextColumn> set_vertex_column(
      std::shared_ptr<IContextColumn> vertices) const override {
    auto ret = std::make_shared<SLVertexPropertyColumn<T>>();
    ret->vertices_col_ = std::dynamic_pointer_cast<SLVertexColumn>(vertices);
    CHECK(ret->vertices_col_ != nullptr);
    ret->vertices_ = ret->vertices_col_.get();
    ret->prop_col_ = prop_col_;
    return ret;
  }

  T get_value(size_t idx) const override {
    return prop_col_.get_view(vertices_->vertices()[idx]);
  }

  size_t size() const override { return vertices_->size(); }

  std::string column_info() const override {
    return "SLVertexPropertyColumn(" + std::to_string(vertices_->label()) +
           ")[" + std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertexProperty;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<T> builder;
    for (auto idx : offsets) {
      builder.push_back_opt(prop_col_.get_view(vertices_->vertices()[idx]));
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  RTAnyType elem_type() const override { return TypedConverter<T>::type(); }
  RTAny get_elem(size_t idx) const override {
    return TypedConverter<T>::from_typed(get_value(idx));
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return false;
  }

 private:
  std::shared_ptr<SLVertexColumn> vertices_col_;
  const SLVertexColumn* vertices_;

  GraphReadInterface::vertex_column_t<T> prop_col_;
};

template <typename T>
class MSVertexPropertyColumn;

template <typename T>
class MLVertexPropertyColumn : public IValueColumn<T> {
 public:
  MLVertexPropertyColumn() = default;
  MLVertexPropertyColumn(const GraphReadInterface& graph,
                         std::shared_ptr<MLVertexColumn> vertices,
                         const std::string& prop_name) {
    vertices_col_ = vertices;
    vertices_ = vertices_col_.get();
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      prop_cols_.push_back(graph.GetVertexColumn<T>(i, prop_name));
    }
  }
  ~MLVertexPropertyColumn() = default;

  const std::shared_ptr<IContextColumn> get_vertex_column() const override {
    return vertices_col_;
  }

  std::shared_ptr<IContextColumn> set_vertex_column(
      std::shared_ptr<IContextColumn> vertices) const override {
    auto ret = std::make_shared<MLVertexPropertyColumn<T>>();
    ret->vertices_col_ = std::dynamic_pointer_cast<MLVertexColumn>(vertices);
    CHECK(ret->vertices_col_ != nullptr);
    ret->vertices_ = ret->vertices_col_.get();
    ret->prop_cols_ = prop_cols_;
    return ret;
  }

  T get_value(size_t idx) const override {
    auto v = vertices_->get_vertex(idx);
    return prop_cols_[v.label()].get_view(v.vid());
  }

  size_t size() const override { return vertices_->size(); }

  std::string column_info() const override {
    std::string labels;
    for (auto label : vertices_->get_labels_set()) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return "MLVertexPropertyColumn(" + labels + ")[" + std::to_string(size()) +
           "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertexProperty;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<T> builder;
    for (auto idx : offsets) {
      auto v = vertices_->get_vertex(idx);
      builder.push_back_opt(prop_cols_[v.label()].get_view(v.vid()));
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  RTAnyType elem_type() const override { return TypedConverter<T>::type(); }
  RTAny get_elem(size_t idx) const override {
    return TypedConverter<T>::from_typed(get_value(idx));
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return false;
  }

 private:
  template <typename _T>
  friend class MSVertexPropertyColumn;

  std::shared_ptr<MLVertexColumn> vertices_col_;
  const MLVertexColumn* vertices_;

  std::vector<GraphReadInterface::vertex_column_t<T>> prop_cols_;
};

template <typename T>
class MSVertexPropertyColumn : public IValueColumn<T> {
 public:
  MSVertexPropertyColumn() = default;
  MSVertexPropertyColumn(const GraphReadInterface& graph,
                         std::shared_ptr<MSVertexColumn> vertices,
                         const std::string& prop_name) {
    vertices_col_ = vertices;
    vertices_ = vertices_col_.get();
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      prop_cols_.push_back(graph.GetVertexColumn<T>(i, prop_name));
    }
  }
  ~MSVertexPropertyColumn() = default;

  const std::shared_ptr<IContextColumn> get_vertex_column() const override {
    return vertices_col_;
  }

  std::shared_ptr<IContextColumn> set_vertex_column(
      std::shared_ptr<IContextColumn> vertices) const override {
    auto casted_vertex_col = std::dynamic_pointer_cast<IVertexColumn>(vertices);
    if (casted_vertex_col->vertex_column_type() ==
        VertexColumnType::kMultiple) {
      auto ret = std::make_shared<MLVertexPropertyColumn<T>>();
      ret->vertices_col_ = std::dynamic_pointer_cast<MLVertexColumn>(vertices);
      CHECK(ret->vertices_col_ != nullptr);
      ret->vertices_ = ret->vertices_col_.get();
      ret->prop_cols_ = prop_cols_;
      return ret;
    } else if (casted_vertex_col->vertex_column_type() ==
               VertexColumnType::kMultiSegment) {
      auto ret = std::make_shared<MSVertexPropertyColumn<T>>();
      ret->vertices_col_ = std::dynamic_pointer_cast<MSVertexColumn>(vertices);
      CHECK(ret->vertices_col_ != nullptr);
      ret->vertices_ = ret->vertices_col_.get();
      ret->prop_cols_ = prop_cols_;
      return ret;
    } else {
      LOG(FATAL) << "not supposed to be single vertex column";
    }
  }

  T get_value(size_t idx) const override {
    auto v = vertices_->get_vertex(idx);
    return prop_cols_[v.label()].get_view(v.vid());
  }

  size_t size() const override { return vertices_->size(); }

  std::string column_info() const override {
    std::string labels;
    for (auto label : vertices_->get_labels_set()) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return "MSVertexPropertyColumn(" + labels + ")[" + std::to_string(size()) +
           "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertexProperty;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<T> builder;
    for (auto idx : offsets) {
      auto v = vertices_->get_vertex(idx);
      builder.push_back_opt(prop_cols_[v.label()].get_view(v.vid()));
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  RTAnyType elem_type() const override { return TypedConverter<T>::type(); }
  RTAny get_elem(size_t idx) const override {
    return TypedConverter<T>::from_typed(get_value(idx));
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    size_t size = vertices_->size();
    if (size == 0) {
      return false;
    }
    if (asc) {
      TopNGenerator<T, TopNAscCmp<T>> generator(limit);
      vertices_->foreach_vertex([&](size_t idx, label_t label, vid_t v) {
        CHECK(!prop_cols_[label].is_null());
        generator.push(prop_cols_[label].get_view(v), idx);
      });
      generator.generate_indices(offsets);
    } else {
      TopNGenerator<T, TopNDescCmp<T>> generator(limit);
      vertices_->foreach_vertex([&](size_t idx, label_t label, vid_t v) {
        CHECK(!prop_cols_[label].is_null());
        generator.push(prop_cols_[label].get_view(v), idx);
      });
      generator.generate_indices(offsets);
    }
    return true;
  }

 private:
  std::shared_ptr<MSVertexColumn> vertices_col_;
  const MSVertexColumn* vertices_;

  std::vector<GraphReadInterface::vertex_column_t<T>> prop_cols_;
};

class SLVertexIdColumn : public IValueColumn<int64_t> {
 public:
  SLVertexIdColumn(const GraphReadInterface& graph,
                   std::shared_ptr<SLVertexColumn> vertices)
      : graph_(graph) {
    vertices_col_ = vertices;
    vertices_ = vertices_col_.get();
  }
  ~SLVertexIdColumn() = default;

  const std::shared_ptr<IContextColumn> get_vertex_column() const override {
    return vertices_col_;
  }

  std::shared_ptr<IContextColumn> set_vertex_column(
      std::shared_ptr<IContextColumn> vertices) const override {
    return std::make_shared<SLVertexIdColumn>(
        graph_, std::dynamic_pointer_cast<SLVertexColumn>(vertices));
  }

  int64_t get_value(size_t idx) const override {
    return graph_.GetVertexId(vertices_->label(), vertices_->vertices()[idx])
        .AsInt64();
  }

  size_t size() const override { return vertices_->size(); }

  std::string column_info() const override {
    return "SLVertexIdColumn(" + std::to_string(vertices_->label()) + ")[" +
           std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertexId;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<int64_t> builder;
    label_t label = vertices_->label();
    for (auto idx : offsets) {
      builder.push_back_opt(
          graph_.GetVertexId(label, vertices_->vertices()[idx]).AsInt64());
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  RTAnyType elem_type() const override {
    return TypedConverter<int64_t>::type();
  }
  RTAny get_elem(size_t idx) const override {
    return TypedConverter<int64_t>::from_typed(get_value(idx));
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override {
    ValueColumnBuilder<int64_t> builder;
    label_t self_label = vertices_->label();
    for (auto v : vertices_->vertices()) {
      builder.push_back_opt(graph_.GetVertexId(self_label, v).AsInt64());
    }
    auto casted_other = std::dynamic_pointer_cast<SLVertexIdColumn>(other);
    if (casted_other) {
      label_t other_label = casted_other->vertices_->label();
      for (auto v : casted_other->vertices_->vertices()) {
        builder.push_back_opt(graph_.GetVertexId(other_label, v).AsInt64());
      }
    } else {
      LOG(FATAL) << "not implemented for other - " << other->column_info();
    }

    return builder.finish();
  }

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return false;
  }

 private:
  std::shared_ptr<SLVertexColumn> vertices_col_;
  const SLVertexColumn* vertices_;

  const GraphReadInterface& graph_;
};

class MLVertexIdColumn : public IValueColumn<int64_t> {
 public:
  MLVertexIdColumn(const GraphReadInterface& graph,
                   std::shared_ptr<MLVertexColumn> vertices)
      : graph_(graph) {
    vertices_col_ = vertices;
    vertices_ = vertices_col_.get();
  }
  ~MLVertexIdColumn() = default;

  const std::shared_ptr<IContextColumn> get_vertex_column() const override {
    return vertices_col_;
  }

  std::shared_ptr<IContextColumn> set_vertex_column(
      std::shared_ptr<IContextColumn> vertices) const override {
    return std::make_shared<MLVertexIdColumn>(
        graph_, std::dynamic_pointer_cast<MLVertexColumn>(vertices));
  }

  int64_t get_value(size_t idx) const override {
    auto v = vertices_->get_vertex(idx);
    return graph_.GetVertexId(v.label(), v.vid()).AsInt64();
  }

  size_t size() const override { return vertices_->size(); }

  std::string column_info() const override {
    std::string labels;
    for (auto label : vertices_->get_labels_set()) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }

    return "MLVertexIdColumn(" + labels + ")[" + std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertexId;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<int64_t> builder;
    for (auto idx : offsets) {
      auto v = vertices_->get_vertex(idx);
      builder.push_back_opt(graph_.GetVertexId(v.label(), v.vid()).AsInt64());
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  RTAnyType elem_type() const override {
    return TypedConverter<int64_t>::type();
  }
  RTAny get_elem(size_t idx) const override {
    return TypedConverter<int64_t>::from_typed(get_value(idx));
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override {
    ValueColumnBuilder<int64_t> builder;
    size_t self_rows = vertices_->size();
    for (size_t i = 0; i < self_rows; ++i) {
      auto v = vertices_->get_vertex(i);
      builder.push_back_opt(graph_.GetVertexId(v.label(), v.vid()).AsInt64());
    }
    auto casted_other = std::dynamic_pointer_cast<MLVertexIdColumn>(other);
    if (casted_other) {
      size_t other_rows = casted_other->vertices_->size();
      for (size_t i = 0; i < other_rows; ++i) {
        auto v = casted_other->vertices_->get_vertex(i);
        builder.push_back_opt(graph_.GetVertexId(v.label(), v.vid()).AsInt64());
      }
    } else {
      LOG(FATAL) << "not implemented for other - " << other->column_info();
    }

    return builder.finish();
  }

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return false;
  }

 private:
  std::shared_ptr<MLVertexColumn> vertices_col_;
  const MLVertexColumn* vertices_;

  const GraphReadInterface& graph_;
};

class MSVertexIdColumn : public IValueColumn<int64_t> {
 public:
  MSVertexIdColumn(const GraphReadInterface& graph,
                   std::shared_ptr<MSVertexColumn> vertices)
      : graph_(graph) {
    vertices_col_ = vertices;
    vertices_ = vertices_col_.get();
  }
  ~MSVertexIdColumn() = default;

  const std::shared_ptr<IContextColumn> get_vertex_column() const override {
    return vertices_col_;
  }

  std::shared_ptr<IContextColumn> set_vertex_column(
      std::shared_ptr<IContextColumn> vertices) const override {
    return std::make_shared<MSVertexIdColumn>(
        graph_, std::dynamic_pointer_cast<MSVertexColumn>(vertices));
  }

  int64_t get_value(size_t idx) const override {
    auto v = vertices_->get_vertex(idx);
    return graph_.GetVertexId(v.label(), v.vid()).AsInt64();
  }

  size_t size() const override { return vertices_->size(); }

  std::string column_info() const override {
    std::string labels;
    for (auto label : vertices_->get_labels_set()) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }

    return "MSVertexIdColumn(" + labels + ")[" + std::to_string(size()) + "]";
  }

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertexId;
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override {
    ValueColumnBuilder<int64_t> builder;
    for (auto idx : offsets) {
      auto v = vertices_->get_vertex(idx);
      builder.push_back_opt(graph_.GetVertexId(v.label(), v.vid()).AsInt64());
    }
    return builder.finish();
  }

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  RTAnyType elem_type() const override {
    return TypedConverter<int64_t>::type();
  }
  RTAny get_elem(size_t idx) const override {
    return TypedConverter<int64_t>::from_typed(get_value(idx));
  }

  ISigColumn* generate_signature() const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return nullptr;
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override {
    ValueColumnBuilder<int64_t> builder;
    size_t self_rows = vertices_->size();
    for (size_t i = 0; i < self_rows; ++i) {
      auto v = vertices_->get_vertex(i);
      builder.push_back_opt(graph_.GetVertexId(v.label(), v.vid()).AsInt64());
    }
    auto casted_other = std::dynamic_pointer_cast<MSVertexIdColumn>(other);
    if (casted_other) {
      size_t other_rows = casted_other->vertices_->size();
      for (size_t i = 0; i < other_rows; ++i) {
        auto v = casted_other->vertices_->get_vertex(i);
        builder.push_back_opt(graph_.GetVertexId(v.label(), v.vid()).AsInt64());
      }
    } else {
      LOG(FATAL) << "not implemented for other - " << other->column_info();
    }

    return builder.finish();
  }

  bool order_by_limit(bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    LOG(FATAL) << "not implemented for " << this->column_info();
    return false;
  }

 private:
  std::shared_ptr<MSVertexColumn> vertices_col_;
  const MSVertexColumn* vertices_;

  const GraphReadInterface& graph_;
};

inline PropertyType get_vertex_property_type(const GraphReadInterface& graph,
                                             label_t label,
                                             const std::string& prop_name) {
  const auto& properties = graph.schema().get_vertex_properties(label);
  const auto& properties_name = graph.schema().get_vertex_property_names(label);
  for (size_t i = 0; i < properties_name.size(); ++i) {
    if (properties_name[i] == prop_name) {
      return properties[i];
    }
  }
  return PropertyType::Empty();
}

inline std::shared_ptr<IContextColumn> create_vertex_property_column(
    const GraphReadInterface& graph, std::shared_ptr<IVertexColumn> vertices,
    const std::string& prop_name, const common::DataType& data_type) {
  if (vertices->vertex_column_type() == VertexColumnType::kSingle) {
    auto sl_vertices = std::dynamic_pointer_cast<SLVertexColumn>(vertices);
    if (prop_name == "id") {
      return std::make_shared<SLVertexIdColumn>(graph, sl_vertices);
    } else {
      auto label = sl_vertices->label();
      PropertyType prop_type =
          get_vertex_property_type(graph, label, prop_name);
      if (data_type == common::DataType::INT32) {
        return std::make_shared<SLVertexPropertyColumn<int32_t>>(
            graph, sl_vertices, prop_name);
      } else if (data_type == common::DataType::INT64) {
        return std::make_shared<SLVertexPropertyColumn<int64_t>>(
            graph, sl_vertices, prop_name);
      } else if (data_type == common::DataType::STRING) {
        return std::make_shared<SLVertexPropertyColumn<std::string_view>>(
            graph, sl_vertices, prop_name);
      } else if (data_type == common::DataType::DATE32) {
        return std::make_shared<SLVertexPropertyColumn<Day>>(graph, sl_vertices,
                                                             prop_name);
      } else if (data_type == common::DataType::TIMESTAMP) {
        return std::make_shared<SLVertexPropertyColumn<Date>>(
            graph, sl_vertices, prop_name);
      } else {
        LOG(FATAL) << "not implemented for " << prop_type
                   << ", data_type = " << data_type;
        return nullptr;
      }
    }
  } else if (vertices->vertex_column_type() == VertexColumnType::kMultiple) {
    auto ml_vertices = std::dynamic_pointer_cast<MLVertexColumn>(vertices);
    if (prop_name == "id") {
      return std::make_shared<MLVertexIdColumn>(graph, ml_vertices);
    } else {
      std::vector<PropertyType> prop_types;
      for (auto label : ml_vertices->get_labels_set()) {
        prop_types.push_back(get_vertex_property_type(graph, label, prop_name));
      }
      bool same_type = true;
      for (size_t i = 1; i < prop_types.size(); ++i) {
        if (prop_types[i] != prop_types[0]) {
          same_type = false;
          break;
        }
      }
      if (!same_type || prop_types.empty()) {
        LOG(INFO) << "not implemented for different property types";
        return nullptr;
      }
      if (data_type == common::DataType::INT32) {
        return std::make_shared<MLVertexPropertyColumn<int32_t>>(
            graph, ml_vertices, prop_name);
      } else if (data_type == common::DataType::INT64) {
        return std::make_shared<MLVertexPropertyColumn<int64_t>>(
            graph, ml_vertices, prop_name);
      } else if (data_type == common::DataType::STRING) {
        return std::make_shared<MLVertexPropertyColumn<std::string_view>>(
            graph, ml_vertices, prop_name);
      } else if (data_type == common::DataType::DATE32) {
        return std::make_shared<MLVertexPropertyColumn<Day>>(graph, ml_vertices,
                                                             prop_name);
      } else if (data_type == common::DataType::TIMESTAMP) {
        return std::make_shared<MLVertexPropertyColumn<Date>>(
            graph, ml_vertices, prop_name);
      } else {
        LOG(FATAL) << "not implemented for " << prop_types[0]
                   << ", data_type = " << data_type;
        return nullptr;
      }
    }
  } else {
    CHECK(vertices->vertex_column_type() == VertexColumnType::kMultiSegment);
    auto ms_vertices = std::dynamic_pointer_cast<MSVertexColumn>(vertices);
    if (prop_name == "id") {
      return std::make_shared<MSVertexIdColumn>(graph, ms_vertices);
    } else {
      std::vector<PropertyType> prop_types;
      for (auto label : ms_vertices->get_labels_set()) {
        prop_types.push_back(get_vertex_property_type(graph, label, prop_name));
      }
      bool same_type = true;
      for (size_t i = 1; i < prop_types.size(); ++i) {
        if (prop_types[i] != prop_types[0]) {
          same_type = false;
          break;
        }
      }
      if (!same_type || prop_types.empty()) {
        LOG(INFO) << "not implemented for different property types";
        return nullptr;
      }
      if (data_type == common::DataType::INT32) {
        return std::make_shared<MSVertexPropertyColumn<int32_t>>(
            graph, ms_vertices, prop_name);
      } else if (data_type == common::DataType::INT64) {
        return std::make_shared<MSVertexPropertyColumn<int64_t>>(
            graph, ms_vertices, prop_name);
      } else if (data_type == common::DataType::STRING) {
        return std::make_shared<MSVertexPropertyColumn<std::string_view>>(
            graph, ms_vertices, prop_name);
      } else if (data_type == common::DataType::DATE32) {
        return std::make_shared<MSVertexPropertyColumn<Day>>(graph, ms_vertices,
                                                             prop_name);
      } else if (data_type == common::DataType::TIMESTAMP) {
        return std::make_shared<MSVertexPropertyColumn<Date>>(
            graph, ms_vertices, prop_name);
      } else {
        LOG(FATAL) << "not implemented for " << prop_types[0]
                   << ", data_type = " << data_type;
        return nullptr;
      }
    }
  }
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_VERTEX_PROPERTY_COLUMNS_H_