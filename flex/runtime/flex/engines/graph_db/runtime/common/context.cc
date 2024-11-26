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

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

Context::Context()
    : head(nullptr), prev_context(nullptr), offset_ptr(nullptr) {}

void Context::clear() {
  columns.clear();
  head.reset();
  offset_ptr = nullptr;
  tag_ids.clear();
  prev_context = nullptr;
}

void Context::update_tag_ids(const std::vector<size_t>& tag_ids) {
  this->tag_ids = tag_ids;
}

void Context::append_tag_id(size_t tag_id) {
  if (std::find(tag_ids.begin(), tag_ids.end(), tag_id) == tag_ids.end()) {
    tag_ids.push_back(tag_id);
  }
}

void Context::set(int alias, std::shared_ptr<IContextColumn> col) {
  head = col;
  if (alias >= 0) {
    if (columns.size() <= static_cast<size_t>(alias)) {
      columns.resize(alias + 1, nullptr);
    }
    assert(columns[alias] == nullptr);
    columns[alias] = col;
  }
}

void Context::set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                                 const std::vector<size_t>& offsets) {
  head.reset();
  head = nullptr;

  if (alias >= 0) {
    if (columns.size() > static_cast<size_t>(alias) &&
        columns[alias] != nullptr) {
      columns[alias].reset();
      columns[alias] = nullptr;
    }
  }

  reshuffle(offsets);
  set(alias, col);
}

void Context::set_with_reshuffle_beta(int alias,
                                      std::shared_ptr<IContextColumn> col,
                                      const std::vector<size_t>& offsets,
                                      const std::set<int>& keep_cols) {
  head.reset();
  head = nullptr;
  if (alias >= 0) {
    if (columns.size() > static_cast<size_t>(alias) &&
        columns[alias] != nullptr) {
      columns[alias].reset();
      columns[alias] = nullptr;
    }
  }
  for (size_t k = 0; k < columns.size(); ++k) {
    if (keep_cols.find(k) == keep_cols.end() && columns[k] != nullptr) {
      columns[k].reset();
      columns[k] = nullptr;
    }
  }

  reshuffle(offsets);

  set(alias, col);
}

void Context::reshuffle(const std::vector<size_t>& offsets) {
  bool head_shuffled = false;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;

  std::unordered_map<std::shared_ptr<IContextColumn>,
                     std::shared_ptr<IContextColumn>>
      vertex_to_new_vertex;
  std::unordered_map<std::shared_ptr<IContextColumn>, int> vertex_ref_count;
  bool head_scaned = false;
  for (auto col : columns) {
    if (col == nullptr) {
      continue;
    }
    if (col == head) {
      head_scaned = true;
    }
    if (col->column_type() == ContextColumnType::kVertexProperty ||
        col->column_type() == ContextColumnType::kVertexId) {
      vertex_ref_count[col->get_vertex_column()]++;
    } else {
      vertex_ref_count[col]++;
    }
  }
  if (!head_scaned && head != nullptr) {
    if (head->column_type() == ContextColumnType::kVertexProperty ||
        head->column_type() == ContextColumnType::kVertexId) {
      vertex_ref_count[head->get_vertex_column()]++;
    } else {
      vertex_ref_count[head]++;
    }
  }

  for (auto col : columns) {
    if (col == nullptr) {
      new_cols.push_back(nullptr);
      continue;
    }
    if (col == head) {
      if (col->column_type() == ContextColumnType::kVertexProperty ||
          col->column_type() == ContextColumnType::kVertexId) {
        auto vertex_col = col->get_vertex_column();
        if (vertex_ref_count[vertex_col] == 1) {
          auto new_col = col->shuffle(offsets);
          new_cols.push_back(new_col);
        } else {
          auto iter = vertex_to_new_vertex.find(vertex_col);
          std::shared_ptr<IContextColumn> new_vertex_col(nullptr);
          if (iter != vertex_to_new_vertex.end()) {
            new_vertex_col = iter->second;
          } else {
            new_vertex_col = vertex_col->shuffle(offsets);
            vertex_to_new_vertex[vertex_col] = new_vertex_col;
          }
          CHECK(new_vertex_col != nullptr);
          new_cols.push_back(col->set_vertex_column(new_vertex_col));
        }
      } else {
        auto iter = vertex_to_new_vertex.find(col);
        if (iter != vertex_to_new_vertex.end()) {
          new_cols.push_back(iter->second);
        } else {
          auto new_col = col->shuffle(offsets);
          new_cols.push_back(new_col);
          vertex_to_new_vertex[col] = new_col;
        }
      }
      head = new_cols.back();
      head_shuffled = true;
    } else {
      if (col->column_type() == ContextColumnType::kVertexProperty ||
          col->column_type() == ContextColumnType::kVertexId) {
        auto vertex_col = col->get_vertex_column();
        if (vertex_ref_count[vertex_col] == 1) {
          auto new_col = col->shuffle(offsets);
          new_cols.push_back(new_col);
        } else {
          auto iter = vertex_to_new_vertex.find(vertex_col);
          std::shared_ptr<IContextColumn> new_vertex_col(nullptr);
          if (iter != vertex_to_new_vertex.end()) {
            new_vertex_col = iter->second;
          } else {
            new_vertex_col = vertex_col->shuffle(offsets);
            vertex_to_new_vertex[vertex_col] = new_vertex_col;
          }
          new_cols.push_back(col->set_vertex_column(new_vertex_col));
        }
      } else {
        auto iter = vertex_to_new_vertex.find(col);
        if (iter != vertex_to_new_vertex.end()) {
          new_cols.push_back(iter->second);
        } else {
          auto new_col = col->shuffle(offsets);
          new_cols.push_back(new_col);
          vertex_to_new_vertex[col] = new_col;
        }
      }
    }
  }
  if (!head_shuffled && head != nullptr) {
    if (head->column_type() == ContextColumnType::kVertexProperty ||
        head->column_type() == ContextColumnType::kVertexId) {
      auto vertex_col = head->get_vertex_column();
      if (vertex_ref_count[vertex_col] == 1) {
        head = head->shuffle(offsets);
      } else {
        auto iter = vertex_to_new_vertex.find(vertex_col);
        std::shared_ptr<IContextColumn> new_vertex_col(nullptr);
        if (iter != vertex_to_new_vertex.end()) {
          new_vertex_col = iter->second;
        } else {
          new_vertex_col = vertex_col->shuffle(offsets);
          vertex_to_new_vertex[vertex_col] = new_vertex_col;
        }
        head = head->set_vertex_column(new_vertex_col);
      }
    } else {
      auto iter = vertex_to_new_vertex.find(head);
      if (iter != vertex_to_new_vertex.end()) {
        head = iter->second;
      } else {
        head = head->shuffle(offsets);
      }
    }
  }
  std::swap(new_cols, columns);
  if (offset_ptr != nullptr) {
    offset_ptr = std::dynamic_pointer_cast<ValueColumn<size_t>>(
        offset_ptr->shuffle(offsets));
  }
}

void Context::optional_reshuffle(const std::vector<size_t>& offsets) {
  bool head_shuffled = false;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;

  for (auto col : columns) {
    if (col == nullptr) {
      new_cols.push_back(nullptr);

      continue;
    }
    if (col == head) {
      head = col->optional_shuffle(offsets);
      new_cols.push_back(head);
      head_shuffled = true;
    } else {
      new_cols.push_back(col->optional_shuffle(offsets));
    }
  }
  if (!head_shuffled && head != nullptr) {
    head = head->optional_shuffle(offsets);
  }
  std::swap(new_cols, columns);
  if (offset_ptr != nullptr) {
    offset_ptr = std::dynamic_pointer_cast<ValueColumn<size_t>>(
        offset_ptr->optional_shuffle(offsets));
  }
}

std::shared_ptr<IContextColumn> Context::get(int alias) {
  if (prev_context == nullptr) {
    if (alias == -1) {
      return head;
    }
    CHECK(static_cast<size_t>(alias) < columns.size());
    return columns[alias];
  } else {
    std::shared_ptr<IContextColumn> ptr{nullptr};
    // find in the current context
    if (alias == -1) {
      ptr = head;
    } else {
      if (static_cast<size_t>(alias) < columns.size()) {
        ptr = columns[alias];
      }
    }
    if (ptr == nullptr) {
      auto ptr = prev_context->get(alias);
      if ((ptr != nullptr) && ptr->size() != offset_ptr->size()) {
        ptr = ptr->shuffle(offset_ptr->data());
      }
      set(alias, ptr);
      return ptr;
    } else {
      return ptr;
    }
  }
}

const std::shared_ptr<IContextColumn> Context::get(int alias) const {
  if (prev_context == nullptr) {
    if (alias == -1) {
      assert(head != nullptr);
      return head;
    }
    CHECK(static_cast<size_t>(alias) < columns.size());
    // return nullptr if the column is not set
    return columns[alias];
  } else {
    std::shared_ptr<IContextColumn> ptr{nullptr};
    // find in the current context
    if (alias == -1) {
      ptr = head;
    } else {
      if (static_cast<size_t>(alias) < columns.size()) {
        ptr = columns[alias];
      }
    }
    if (ptr == nullptr) {
      auto ptr = prev_context->get(alias);
      if ((ptr != nullptr) && ptr->size() != offset_ptr->size()) {
        ptr = ptr->shuffle(offset_ptr->data());
      }
      // ???
      // set(alias, ptr);
      return ptr;
    } else {
      return ptr;
    }
  }
}

void Context::remove(int alias) {
  if (alias == -1) {
    for (auto& col : columns) {
      if (col == head) {
        col = nullptr;
      }
    }
    head = nullptr;
  } else if (static_cast<size_t>(alias) < columns.size() && alias >= 0) {
    if (head == columns[alias]) {
      head = nullptr;
    }
    columns[alias] = nullptr;
  }
}

size_t Context::row_num() const {
  for (auto col : columns) {
    if (col != nullptr) {
      return col->size();
    }
  }
  if (head != nullptr) {
    return head->size();
  }
  if (prev_context != nullptr) {
    return prev_context->row_num();
  }

  return 0;
}

bool Context::exist(int alias) const {
  if (alias == -1 && head != nullptr) {
    return true;
  }
  if (static_cast<size_t>(alias) >= columns.size()) {
    return false;
  }
  return columns[alias] != nullptr;
}

void Context::desc(const std::string& info) const {
  if (!info.empty()) {
    LOG(INFO) << info;
  }
  for (size_t col_i = 0; col_i < col_num(); ++col_i) {
    if (columns[col_i] != nullptr) {
      LOG(INFO) << "\tcol-" << col_i << ": " << columns[col_i]->column_info();
    }
  }
  LOG(INFO) << "\thead: " << ((head == nullptr) ? "NULL" : head->column_info());
}

void Context::show(const GraphReadInterface& graph) const {
  size_t rn = row_num();
  size_t cn = col_num();
  for (size_t ri = 0; ri < rn; ++ri) {
    std::string line;
    for (size_t ci = 0; ci < cn; ++ci) {
      if (columns[ci] != nullptr &&
          columns[ci]->column_type() == ContextColumnType::kVertex) {
        auto v = std::dynamic_pointer_cast<IVertexColumn>(columns[ci])
                     ->get_vertex(ri);
        int64_t id = graph.GetVertexId(v.label_, v.vid_).AsInt64();
        line += std::to_string(id);
        line += ", ";
      } else if (columns[ci] != nullptr) {
        line += columns[ci]->get_elem(ri).to_string();
        line += ", ";
      }
    }
    LOG(INFO) << line;
  }
}

void Context::set_prev_context(Context* prev_context) {
  this->prev_context = prev_context;
  CHECK(prev_context != this);
  ValueColumnBuilder<size_t> builder;
  size_t prev_row_num = prev_context->row_num();
  builder.reserve(prev_row_num);
  for (size_t i = 0; i < prev_row_num; ++i) {
    builder.push_back_opt(i);
  }
  offset_ptr = std::dynamic_pointer_cast<ValueColumn<size_t>>(builder.finish());
}

Context Context::union_ctx(const Context& other) const {
  Context ctx;
  CHECK(columns.size() == other.columns.size());
  for (size_t i = 0; i < col_num(); ++i) {
    if (columns[i] != nullptr) {
      if (head == columns[i]) {
        auto col = columns[i]->union_col(other.get(i));
        ctx.set(i, col);
        ctx.head = col;
      } else {
        ctx.set(i, columns[i]->union_col(other.get(i)));
      }
    }
  }
  if (offset_ptr != nullptr) {
    CHECK(other.offset_ptr != nullptr);
    ctx.offset_ptr = std::dynamic_pointer_cast<ValueColumn<size_t>>(
        offset_ptr->union_col(other.offset_ptr));
  }
  return ctx;
}
const ValueColumn<size_t>& Context::get_offsets() const { return *offset_ptr; }
size_t Context::col_num() const { return columns.size(); }

}  // namespace runtime

}  // namespace gs
