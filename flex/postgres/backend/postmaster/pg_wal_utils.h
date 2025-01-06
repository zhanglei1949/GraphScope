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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_PG_PG_WAL_UTILS_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_PG_PG_WAL_UTILS_H_

#include <string>

// #ifdef __cplusplus
// extern "C" {
// #endif


// #ifdef __cplusplus
// }
// #endif

/**
 * The wrapper of the postgres wal related functions.
 */
// void StartPostMaster(const std::string& path);
bool WriteWal(const char* data, size_t length);

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_PG_PG_WAL_UTILS_H_
