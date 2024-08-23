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

#include "flex/engines/graph_db/runtime/common/operators/join.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "parallel_hashmap/phmap.h"

// #define DEBUG_JOIN

namespace gs {

namespace runtime {

#if 0
struct vertex_pair {
  vertex_pair(const std::pair<label_t, vid_t>& a,
              const std::pair<label_t, vid_t>& b)
      : a_label(a.first), b_label(b.first), a_id(a.second), b_id(b.second) {}

  bool operator<(const vertex_pair& rhs) const {
    if (a_id == rhs.a_id) {
      if (b_id == rhs.b_id) {
        if (a_label == rhs.a_label) {
          if (b_label == rhs.b_label) {
            return false;
          } else {
            return b_label < rhs.b_label;
          }
        } else {
          return a_label < rhs.a_label;
        }
      } else {
        return b_id < rhs.b_id;
      }
    } else {
      return a_id < rhs.a_id;
    }
  }

  bool operator==(const vertex_pair& rhs) const {
    return (a_label == rhs.a_label) && (b_label == rhs.b_label) &&
           (a_id == rhs.a_id) && (b_id && rhs.b_id);
  }

  struct Hash {
    size_t operator()(const vertex_pair& obj) const {
      size_t ret = obj.a_id;
      ret <<= 32;
      ret |= (obj.b_id);
      return ret;
    }
  };

  label_t a_label;
  label_t b_label;
  vid_t a_id;
  vid_t b_id;
};
#else
using vertex_pair =
    std::pair<std::pair<label_t, vid_t>, std::pair<label_t, vid_t>>;
#endif

Context Join::join(Context&& ctx, Context&& ctx2, const JoinParams& params) {
  CHECK(params.left_columns.size() == params.right_columns.size())
      << "Join columns size mismatch";
#ifdef DEBUG_JOIN
  double t = -grape::GetCurrentTime();
#endif
  if (params.join_type == JoinKind::kSemiJoin ||
      params.join_type == JoinKind::kAntiJoin) {
#ifdef DEBUG_JOIN
    LOG(INFO) << "join type 0";
#endif
    size_t right_size = ctx2.row_num();
    std::set<std::string> right_set;
    std::vector<size_t> offset;

    for (size_t r_i = 0; r_i < right_size; ++r_i) {
      std::vector<char> bytes;
      Encoder encoder(bytes);
      for (size_t i = 0; i < params.right_columns.size(); i++) {
        auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
        val.encode_sig(val.type(), encoder);
        encoder.put_byte('#');
      }
      std::string cur(bytes.begin(), bytes.end());
      right_set.insert(cur);
    }

    size_t left_size = ctx.row_num();
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      std::vector<char> bytes;
      Encoder encoder(bytes);
      for (size_t i = 0; i < params.left_columns.size(); i++) {
        auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
        val.encode_sig(val.type(), encoder);
        encoder.put_byte('#');
      }
      std::string cur(bytes.begin(), bytes.end());
      if (params.join_type == JoinKind::kSemiJoin) {
        if (right_set.find(cur) != right_set.end()) {
          offset.push_back(r_i);
        }
      } else {
        if (right_set.find(cur) == right_set.end()) {
          offset.push_back(r_i);
        }
      }
    }
    ctx.reshuffle(offset);
#ifdef DEBUG_JOIN
    t += grape::GetCurrentTime();
    LOG(INFO) << "join takes: " << t << " s";
#endif
    return ctx;
  } else if (params.join_type == JoinKind::kInnerJoin) {
#ifdef DEBUG_JOIN
    LOG(INFO) << "join type 1, keys size: " << params.right_columns.size()
              << ", left size = " << ctx.row_num()
              << ", right size = " << ctx2.row_num();
    for (size_t k = 0; k < params.left_columns.size(); ++k) {
      LOG(INFO) << "left key - " << k << ": "
                << ctx.get(params.left_columns[k])->column_info();
    }
    for (size_t k = 0; k < params.right_columns.size(); ++k) {
      LOG(INFO) << "right key - " << k << ": "
                << ctx2.get(params.right_columns[k])->column_info();
    }
#endif
    std::vector<size_t> left_offset, right_offset;
    if (params.right_columns.size() == 1) {
      auto left_col = ctx.get(params.left_columns[0]);
      auto right_col = ctx2.get(params.right_columns[0]);
      if (left_col->column_type() == ContextColumnType::kVertex) {
        CHECK(right_col->column_type() == ContextColumnType::kVertex);

        auto casted_left_col =
            std::dynamic_pointer_cast<IVertexColumn>(left_col);
        auto casted_right_col =
            std::dynamic_pointer_cast<IVertexColumn>(right_col);

        size_t left_size = casted_left_col->size();
        size_t right_size = casted_right_col->size();

        if (left_size < right_size) {
// if (false) {
// std::map<std::pair<label_t, vid_t>, std::vector<size_t>> left_map;
#if 0
          phmap::flat_hash_map<std::pair<label_t, vid_t>, std::vector<size_t>>
              left_map;
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            left_map[casted_left_col->get_vertex(r_i)].emplace_back(r_i);
          }
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            auto iter = left_map.find(casted_right_col->get_vertex(r_i));
            if (iter != left_map.end()) {
              for (auto idx : iter->second) {
                left_offset.emplace_back(idx);
                right_offset.emplace_back(r_i);
              }
            }
          }
#else
          phmap::flat_hash_set<std::pair<label_t, vid_t>> left_set;
          phmap::flat_hash_map<std::pair<label_t, vid_t>, std::vector<size_t>>
              right_map;
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            left_set.emplace(casted_left_col->get_vertex(r_i));
          }
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            auto cur = casted_right_col->get_vertex(r_i);
            if (left_set.find(cur) != left_set.end()) {
              right_map[cur].emplace_back(r_i);
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            auto iter = right_map.find(casted_left_col->get_vertex(r_i));
            if (iter != right_map.end()) {
              for (auto idx : iter->second) {
                left_offset.emplace_back(r_i);
                right_offset.emplace_back(idx);
              }
            }
          }
#endif
        } else {
          // std::map<std::pair<label_t, vid_t>, std::vector<size_t>> right_map;
          phmap::flat_hash_map<std::pair<label_t, vid_t>, std::vector<size_t>>
              right_map;
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            right_map[casted_right_col->get_vertex(r_i)].emplace_back(r_i);
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            auto iter = right_map.find(casted_left_col->get_vertex(r_i));
            if (iter != right_map.end()) {
              for (auto idx : iter->second) {
                left_offset.emplace_back(r_i);
                right_offset.emplace_back(idx);
              }
            }
          }
        }
      } else {
        size_t right_size = ctx2.row_num();
        std::map<std::string, std::vector<size_t>> right_set;

        for (size_t r_i = 0; r_i < right_size; ++r_i) {
          std::vector<char> bytes;
          Encoder encoder(bytes);
          for (size_t i = 0; i < params.right_columns.size(); i++) {
            auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
            val.encode_sig(val.type(), encoder);
            encoder.put_byte('#');
          }
          std::string cur(bytes.begin(), bytes.end());
          right_set[cur].emplace_back(r_i);
        }

        size_t left_size = ctx.row_num();
        for (size_t r_i = 0; r_i < left_size; ++r_i) {
          std::vector<char> bytes;
          Encoder encoder(bytes);
          for (size_t i = 0; i < params.left_columns.size(); i++) {
            auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
            val.encode_sig(val.type(), encoder);
            encoder.put_byte('#');
          }
          std::string cur(bytes.begin(), bytes.end());
          if (right_set.find(cur) != right_set.end()) {
            for (auto right : right_set[cur]) {
              left_offset.push_back(r_i);
              right_offset.push_back(right);
            }
          }
        }
      }
    } else {
      size_t right_size = ctx2.row_num();
      std::map<std::string, std::vector<size_t>> right_set;

      for (size_t r_i = 0; r_i < right_size; ++r_i) {
        std::vector<char> bytes;
        Encoder encoder(bytes);
        for (size_t i = 0; i < params.right_columns.size(); i++) {
          auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
          val.encode_sig(val.type(), encoder);
          encoder.put_byte('#');
        }
        std::string cur(bytes.begin(), bytes.end());
        right_set[cur].emplace_back(r_i);
      }

      size_t left_size = ctx.row_num();
      for (size_t r_i = 0; r_i < left_size; ++r_i) {
        std::vector<char> bytes;
        Encoder encoder(bytes);
        for (size_t i = 0; i < params.left_columns.size(); i++) {
          auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
          val.encode_sig(val.type(), encoder);
          encoder.put_byte('#');
        }
        std::string cur(bytes.begin(), bytes.end());
        if (right_set.find(cur) != right_set.end()) {
          for (auto right : right_set[cur]) {
            left_offset.push_back(r_i);
            right_offset.push_back(right);
          }
        }
      }
    }
    ctx.reshuffle(left_offset);
    ctx2.reshuffle(right_offset);
    Context ret;
    for (size_t i = 0; i < ctx.col_num(); i++) {
      ret.set(i, ctx.get(i));
    }
    for (size_t i = 0; i < ctx2.col_num(); i++) {
      if (i >= ret.col_num() || ret.get(i) == nullptr) {
        ret.set(i, ctx2.get(i));
      }
    }
#ifdef DEBUG_JOIN
    t += grape::GetCurrentTime();
    LOG(INFO) << "join takes: " << t << " s";
#endif
    return ret;
  } else if (params.join_type == JoinKind::kLeftOuterJoin) {
#ifdef DEBUG_JOIN
    LOG(INFO) << "join type 2, keys size: " << params.right_columns.size()
              << ", left size = " << ctx.row_num()
              << ", right size = " << ctx2.row_num();
    for (size_t k = 0; k < params.left_columns.size(); ++k) {
      LOG(INFO) << "left key - " << k << ": "
                << ctx.get(params.left_columns[k])->column_info();
    }
    for (size_t k = 0; k < params.right_columns.size(); ++k) {
      LOG(INFO) << "right key - " << k << ": "
                << ctx2.get(params.right_columns[k])->column_info();
    }
#endif
    if (params.right_columns.size() == 1) {
      auto left_col = ctx.get(params.left_columns[0]);
      auto right_col = ctx2.get(params.right_columns[0]);
      if (left_col->column_type() == ContextColumnType::kVertex) {
        CHECK(right_col->column_type() == ContextColumnType::kVertex);

        auto casted_left_col =
            std::dynamic_pointer_cast<IVertexColumn>(left_col);
        auto casted_right_col =
            std::dynamic_pointer_cast<IVertexColumn>(right_col);

#if 0
        size_t left_size = casted_left_col->size();
        size_t right_size = casted_right_col->size();
        std::map<std::pair<label_t, vid_t>, std::vector<vid_t>> right_map;
        if (left_size > 0) {
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            right_map[casted_right_col->get_vertex(r_i)].emplace_back(r_i);
          }
        }

        std::vector<std::shared_ptr<IOptionalContextColumnBuilder>> builders;
        for (size_t i = 0; i < ctx2.col_num(); ++i) {
          if (i != static_cast<size_t>(params.right_columns[0]) &&
              ctx2.get(i) != nullptr) {
            builders.emplace_back(ctx2.get(i)->optional_builder());
          } else {
            builders.emplace_back(nullptr);
          }
        }

        std::vector<size_t> offsets;
        for (size_t r_i = 0; r_i < left_size; ++r_i) {
          auto cur = casted_left_col->get_vertex(r_i);
          auto iter = right_map.find(cur);
          if (iter == right_map.end()) {
            for (size_t i = 0; i < ctx2.col_num(); ++i) {
              if (builders[i] != nullptr) {
                builders[i]->push_back_null();
              }
            }
            offsets.emplace_back(r_i);
          } else {
            for (auto idx : iter->second) {
              for (size_t i = 0; i < ctx2.col_num(); ++i) {
                if (builders[i] != nullptr) {
                  builders[i]->push_back_elem(ctx2.get(i)->get_elem(idx));
                }
              }
              offsets.emplace_back(r_i);
            }
          }
        }
        ctx.reshuffle(offsets);
        for (size_t i = 0; i < ctx2.col_num(); ++i) {
          if (builders[i] != nullptr) {
            ctx.set(i, builders[i]->finish());
          } else if (i >= ctx.col_num()) {
            ctx.set(i, nullptr);
          }
        }
#else
        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;

        size_t left_size = casted_left_col->size();
        size_t right_size = casted_right_col->size();
        if (left_size < right_size) {
          // if (false) {
          // std::map<std::pair<label_t, vid_t>, std::vector<vid_t>> left_map;
#if 0
          phmap::flat_hash_map<std::pair<label_t, vid_t>, std::vector<vid_t>>
              left_map;
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            left_map[casted_left_col->get_vertex(r_i)].emplace_back(r_i);
          }
          std::vector<bool> picked_rows(left_size, false);
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            auto cur = casted_right_col->get_vertex(r_i);
            auto iter = left_map.find(cur);
            if (iter != left_map.end()) {
              for (auto idx : iter->second) {
                left_offsets.emplace_back(idx);
                right_offsets.emplace_back(r_i);
                picked_rows[idx] = true;
              }
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            if (!picked_rows[r_i]) {
              left_offsets.emplace_back(r_i);
              right_offsets.emplace_back(std::numeric_limits<size_t>::max());
            }
          }
#else
          phmap::flat_hash_set<std::pair<label_t, vid_t>> left_set;
          phmap::flat_hash_map<std::pair<label_t, vid_t>, std::vector<size_t>>
              right_map;
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            left_set.emplace(casted_left_col->get_vertex(r_i));
          }
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            auto cur = casted_right_col->get_vertex(r_i);
            if (left_set.find(cur) != left_set.end()) {
              right_map[cur].emplace_back(r_i);
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            auto cur = casted_left_col->get_vertex(r_i);
            auto iter = right_map.find(cur);
            if (iter == right_map.end()) {
              left_offsets.emplace_back(r_i);
              right_offsets.emplace_back(std::numeric_limits<size_t>::max());
            } else {
              for (auto idx : iter->second) {
                left_offsets.emplace_back(r_i);
                right_offsets.emplace_back(idx);
              }
            }
          }
#endif
        } else {
          // std::map<std::pair<label_t, vid_t>, std::vector<vid_t>> right_map;
          phmap::flat_hash_map<std::pair<label_t, vid_t>, std::vector<vid_t>>
              right_map;
          if (left_size > 0) {
            for (size_t r_i = 0; r_i < right_size; ++r_i) {
              right_map[casted_right_col->get_vertex(r_i)].emplace_back(r_i);
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            auto cur = casted_left_col->get_vertex(r_i);
            auto iter = right_map.find(cur);
            if (iter == right_map.end()) {
              left_offsets.emplace_back(r_i);
              right_offsets.emplace_back(std::numeric_limits<size_t>::max());
            } else {
              for (auto idx : iter->second) {
                left_offsets.emplace_back(r_i);
                right_offsets.emplace_back(idx);
              }
            }
          }
        }
        ctx.reshuffle(left_offsets);
        ctx2.remove(params.right_columns[0]);
        ctx2.optional_reshuffle(right_offsets);
        for (size_t i = 0; i < ctx2.col_num(); ++i) {
          if (ctx2.get(i) != nullptr) {
            ctx.set(i, ctx2.get(i));
          }
        }
#endif
      } else {
        size_t right_size = ctx2.row_num();
        std::map<std::string, std::vector<vid_t>> right_map;
        if (ctx.row_num() > 0) {
          for (size_t r_i = 0; r_i < right_size; r_i++) {
            std::vector<char> bytes;
            Encoder encoder(bytes);
            for (size_t i = 0; i < params.right_columns.size(); i++) {
              auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
              val.encode_sig(val.type(), encoder);
              encoder.put_byte('#');
            }
            std::string cur(bytes.begin(), bytes.end());
            right_map[cur].emplace_back(r_i);
          }
        }

        std::vector<std::shared_ptr<IOptionalContextColumnBuilder>> builders;
        for (size_t i = 0; i < ctx2.col_num(); i++) {
          if (std::find(params.right_columns.begin(),
                        params.right_columns.end(),
                        i) == params.right_columns.end() &&
              ctx2.get(i) != nullptr) {
            builders.emplace_back(ctx2.get(i)->optional_builder());
          } else {
            builders.emplace_back(nullptr);
          }
        }

        std::vector<size_t> offsets;
        size_t left_size = ctx.row_num();
        for (size_t r_i = 0; r_i < left_size; r_i++) {
          std::vector<char> bytes;
          Encoder encoder(bytes);
          for (size_t i = 0; i < params.left_columns.size(); i++) {
            auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
            val.encode_sig(val.type(), encoder);
            encoder.put_byte('#');
          }
          std::string cur(bytes.begin(), bytes.end());
          if (right_map.find(cur) == right_map.end()) {
            for (size_t i = 0; i < ctx2.col_num(); i++) {
              if (builders[i] != nullptr) {
                builders[i]->push_back_null();
              }
            }
            offsets.emplace_back(r_i);
          } else {
            for (auto idx : right_map[cur]) {
              for (size_t i = 0; i < ctx2.col_num(); i++) {
                if (builders[i] != nullptr) {
                  builders[i]->push_back_elem(ctx2.get(i)->get_elem(idx));
                }
              }
              offsets.emplace_back(r_i);
            }
          }
        }
        ctx.reshuffle(offsets);
        for (size_t i = 0; i < ctx2.col_num(); i++) {
          if (builders[i] != nullptr) {
            ctx.set(i, builders[i]->finish());
          } else if (i >= ctx.col_num()) {
            ctx.set(i, nullptr);
          }
        }
      }
    } else if (params.right_columns.size() == 2) {
      auto left_col0 = ctx.get(params.left_columns[0]);
      auto left_col1 = ctx.get(params.left_columns[1]);
      auto right_col0 = ctx2.get(params.right_columns[0]);
      auto right_col1 = ctx2.get(params.right_columns[1]);

      if (left_col0->column_type() == ContextColumnType::kVertex &&
          left_col1->column_type() == ContextColumnType::kVertex) {
        CHECK(right_col0->column_type() == ContextColumnType::kVertex);
        CHECK(right_col1->column_type() == ContextColumnType::kVertex);
        auto casted_left_col0 =
            std::dynamic_pointer_cast<IVertexColumn>(left_col0);
        auto casted_left_col1 =
            std::dynamic_pointer_cast<IVertexColumn>(left_col1);
        auto casted_right_col0 =
            std::dynamic_pointer_cast<IVertexColumn>(right_col0);
        auto casted_right_col1 =
            std::dynamic_pointer_cast<IVertexColumn>(right_col1);

        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        size_t left_size = casted_left_col0->size();
        size_t right_size = casted_right_col0->size();

        if (left_size < right_size) {
// if (false) {
// std::map<vertex_pair, std::vector<vid_t>> left_map;
#if 0
          phmap::flat_hash_map<vertex_pair, std::vector<vid_t>> left_map;
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            vertex_pair cur(casted_left_col0->get_vertex(r_i),
                            casted_left_col1->get_vertex(r_i));
            left_map[cur].emplace_back(r_i);
          }
          std::vector<bool> picked_rows(left_size, false);
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            vertex_pair cur(casted_right_col0->get_vertex(r_i),
                            casted_right_col1->get_vertex(r_i));
            auto iter = left_map.find(cur);
            if (iter != left_map.end()) {
              for (auto idx : iter->second) {
                left_offsets.emplace_back(idx);
                right_offsets.emplace_back(r_i);
                picked_rows[idx] = true;
              }
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            if (!picked_rows[r_i]) {
              left_offsets.emplace_back(r_i);
              right_offsets.emplace_back(std::numeric_limits<size_t>::max());
            }
          }
#else
          phmap::flat_hash_set<vertex_pair> left_set;
          phmap::flat_hash_map<vertex_pair, std::vector<size_t>> right_map;
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            vertex_pair cur(casted_left_col0->get_vertex(r_i),
                            casted_left_col1->get_vertex(r_i));
            left_set.emplace(cur);
          }
          for (size_t r_i = 0; r_i < right_size; ++r_i) {
            vertex_pair cur(casted_right_col0->get_vertex(r_i),
                            casted_right_col1->get_vertex(r_i));
            if (left_set.find(cur) != left_set.end()) {
              right_map[cur].emplace_back(r_i);
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            vertex_pair cur(casted_left_col0->get_vertex(r_i),
                            casted_left_col1->get_vertex(r_i));
            auto iter = right_map.find(cur);
            if (iter == right_map.end()) {
              left_offsets.emplace_back(r_i);
              right_offsets.emplace_back(std::numeric_limits<size_t>::max());
            } else {
              for (auto idx : iter->second) {
                left_offsets.emplace_back(r_i);
                right_offsets.emplace_back(idx);
              }
            }
          }
#endif
        } else {
          // std::map<vertex_pair, std::vector<vid_t>> right_map;
          phmap::flat_hash_map<vertex_pair, std::vector<vid_t>> right_map;
          if (left_size > 0) {
            for (size_t r_i = 0; r_i < right_size; ++r_i) {
              vertex_pair cur(casted_right_col0->get_vertex(r_i),
                              casted_right_col1->get_vertex(r_i));
              right_map[cur].emplace_back(r_i);
            }
          }
          for (size_t r_i = 0; r_i < left_size; ++r_i) {
            vertex_pair cur(casted_left_col0->get_vertex(r_i),
                            casted_left_col1->get_vertex(r_i));
            auto iter = right_map.find(cur);
            if (iter == right_map.end()) {
              left_offsets.emplace_back(r_i);
              right_offsets.emplace_back(std::numeric_limits<size_t>::max());
            } else {
              for (auto idx : iter->second) {
                left_offsets.emplace_back(r_i);
                right_offsets.emplace_back(idx);
              }
            }
          }
        }
        ctx.reshuffle(left_offsets);
        ctx2.remove(params.right_columns[0]);
        ctx2.remove(params.right_columns[1]);
        ctx2.optional_reshuffle(right_offsets);
        for (size_t i = 0; i < ctx2.col_num(); ++i) {
          if (ctx2.get(i) != nullptr) {
            ctx.set(i, ctx2.get(i));
          }
        }
      } else {
        size_t right_size = ctx2.row_num();
        std::map<std::string, std::vector<vid_t>> right_map;
        if (ctx.row_num() > 0) {
          for (size_t r_i = 0; r_i < right_size; r_i++) {
            std::vector<char> bytes;
            Encoder encoder(bytes);
            for (size_t i = 0; i < params.right_columns.size(); i++) {
              auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
              val.encode_sig(val.type(), encoder);
              encoder.put_byte('#');
            }
            std::string cur(bytes.begin(), bytes.end());
            right_map[cur].emplace_back(r_i);
          }
        }

        std::vector<std::shared_ptr<IOptionalContextColumnBuilder>> builders;
        for (size_t i = 0; i < ctx2.col_num(); i++) {
          if (std::find(params.right_columns.begin(),
                        params.right_columns.end(),
                        i) == params.right_columns.end() &&
              ctx2.get(i) != nullptr) {
            builders.emplace_back(ctx2.get(i)->optional_builder());
          } else {
            builders.emplace_back(nullptr);
          }
        }

        std::vector<size_t> offsets;
        size_t left_size = ctx.row_num();
        for (size_t r_i = 0; r_i < left_size; r_i++) {
          std::vector<char> bytes;
          Encoder encoder(bytes);
          for (size_t i = 0; i < params.left_columns.size(); i++) {
            auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
            val.encode_sig(val.type(), encoder);
            encoder.put_byte('#');
          }
          std::string cur(bytes.begin(), bytes.end());
          if (right_map.find(cur) == right_map.end()) {
            for (size_t i = 0; i < ctx2.col_num(); i++) {
              if (builders[i] != nullptr) {
                builders[i]->push_back_null();
              }
            }
            offsets.emplace_back(r_i);
          } else {
            for (auto idx : right_map[cur]) {
              for (size_t i = 0; i < ctx2.col_num(); i++) {
                if (builders[i] != nullptr) {
                  builders[i]->push_back_elem(ctx2.get(i)->get_elem(idx));
                }
              }
              offsets.emplace_back(r_i);
            }
          }
        }
        ctx.reshuffle(offsets);
        for (size_t i = 0; i < ctx2.col_num(); i++) {
          if (builders[i] != nullptr) {
            ctx.set(i, builders[i]->finish());
          } else if (i >= ctx.col_num()) {
            ctx.set(i, nullptr);
          }
        }
      }
    } else {
      size_t right_size = ctx2.row_num();
      std::map<std::string, std::vector<vid_t>> right_map;
      if (ctx.row_num() > 0) {
        for (size_t r_i = 0; r_i < right_size; r_i++) {
          std::vector<char> bytes;
          Encoder encoder(bytes);
          for (size_t i = 0; i < params.right_columns.size(); i++) {
            auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
            val.encode_sig(val.type(), encoder);
            encoder.put_byte('#');
          }
          std::string cur(bytes.begin(), bytes.end());
          right_map[cur].emplace_back(r_i);
        }
      }

      std::vector<std::shared_ptr<IOptionalContextColumnBuilder>> builders;
      for (size_t i = 0; i < ctx2.col_num(); i++) {
        if (std::find(params.right_columns.begin(), params.right_columns.end(),
                      i) == params.right_columns.end() &&
            ctx2.get(i) != nullptr) {
          builders.emplace_back(ctx2.get(i)->optional_builder());
        } else {
          builders.emplace_back(nullptr);
        }
      }

      std::vector<size_t> offsets;
      size_t left_size = ctx.row_num();
      for (size_t r_i = 0; r_i < left_size; r_i++) {
        std::vector<char> bytes;
        Encoder encoder(bytes);
        for (size_t i = 0; i < params.left_columns.size(); i++) {
          auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
          val.encode_sig(val.type(), encoder);
          encoder.put_byte('#');
        }
        std::string cur(bytes.begin(), bytes.end());
        if (right_map.find(cur) == right_map.end()) {
          for (size_t i = 0; i < ctx2.col_num(); i++) {
            if (builders[i] != nullptr) {
              builders[i]->push_back_null();
            }
          }
          offsets.emplace_back(r_i);
        } else {
          for (auto idx : right_map[cur]) {
            for (size_t i = 0; i < ctx2.col_num(); i++) {
              if (builders[i] != nullptr) {
                builders[i]->push_back_elem(ctx2.get(i)->get_elem(idx));
              }
            }
            offsets.emplace_back(r_i);
          }
        }
      }
      ctx.reshuffle(offsets);
      for (size_t i = 0; i < ctx2.col_num(); i++) {
        if (builders[i] != nullptr) {
          ctx.set(i, builders[i]->finish());
        } else if (i >= ctx.col_num()) {
          ctx.set(i, nullptr);
        }
      }
    }
#ifdef DEBUG_JOIN
    t += grape::GetCurrentTime();
    LOG(INFO) << "join takes: " << t << " s";
#endif
    return ctx;
  }
  LOG(FATAL) << "Unsupported join type";

  return Context();
}
}  // namespace runtime
}  // namespace gs