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

#include "flex/storages/metadata/sqlite_metadata_store.h"

namespace gs {

SqliteMetadataStore::SqliteMetadataStore()
    : db_(nullptr),
      insert_graph_meta_stmt_(nullptr),
      get_graph_meta_stmt_(nullptr),
      get_all_graph_meta_stmt_(nullptr),
      delete_graph_meta_stmt_(nullptr),
      insert_plugin_meta_stmt_(nullptr),
      get_plugin_meta_stmt_(nullptr),
      get_all_plugin_meta_stmt_(nullptr),
      delete_plugin_meta_stmt_(nullptr),
      delete_plugin_meta_graph_id_stmt_(nullptr),
      insert_job_meta_stmt_(nullptr),
      get_job_meta_stmt_(nullptr),
      get_all_job_meta_stmt_(nullptr),
      delete_job_meta_stmt_(nullptr),
      lock_graph_indices_(nullptr),
      unlock_graph_indices_(nullptr),
      get_graph_indices_locked_(nullptr),
      lock_graph_plugins_(nullptr),
      unlock_graph_plugins_(nullptr),
      get_graph_plugins_locked_(nullptr),
      set_running_graph_stmt_(nullptr),
      get_running_graph_stmt_(nullptr),
      clear_running_graph_stmt_(nullptr) {}

SqliteMetadataStore::~SqliteMetadataStore() { Close(); }

Result<bool> SqliteMetadataStore::Open() {
  LOG(INFO) << "Open database: " << DB_PATH;
  if (db_) {
    return Result<bool>(
        Status(StatusCode::ReopenError, "The db is already open"), false);
  }
  // If DB_PATH is empty, then a new database will be created.
  // If DB_PATH is not empty, then the database will be opened.
  int rc = sqlite3_open_v2(
      DB_PATH, &db_,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
      nullptr);
  if (rc) {
    LOG(ERROR) << "Can't open database: " << sqlite3_errmsg(db_);
    return Status(StatusCode::ErrorOpenMeta,
                  "Can't open database: " + std::string(sqlite3_errmsg(db_)));
  }
  LOG(INFO) << "Opened database successfully: " << DB_PATH;

  // Prepare the table, if not exists.
  RETURN_IF_NOT_OK(createTables());
  RETURN_IF_NOT_OK(prepare_statements());
  return true;
}

Result<bool> SqliteMetadataStore::Close() {
  if (db_) {
    // first finalize all the prepared statements
    finalize_statements();
    int rc = sqlite3_close(db_);
    if (rc) {
      LOG(ERROR) << "Can't close database: " << sqlite3_errmsg(db_);
      return Status(
          StatusCode::ErrorOpenMeta,
          "Can't close database: " + std::string(sqlite3_errmsg(db_)));
    }
    LOG(INFO) << "Closed database successfully: " << DB_PATH;
    db_ = nullptr;
  }
  return true;
}

Result<GraphId> SqliteMetadataStore::CreateGraphMeta(
    const CreateGraphMetaRequest& request) {
  // binding the parameters to the prepared statement
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_graph_meta_stmt_, 1,
                                    request.name);  // name
  SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_graph_meta_stmt_, 2,
                                       request.GetIsBuiltin());  // is_builtin
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_graph_meta_stmt_, 3,
                                    request.description);  // description
  if (request.data_update_time.has_value()) {
    SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_graph_meta_stmt_, 4,
                                         request.data_update_time.value());
  } else {
    SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_graph_meta_stmt_, 4, 0);
  }
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_graph_meta_stmt_, 5,
                                    request.graph_schema);  // schema
  SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_graph_meta_stmt_, 6,
                                       false);  // indices_locked
  SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_graph_meta_stmt_, 7,
                                       false);  // plugins_locked
  int rc = sqlite3_step(insert_graph_meta_stmt_);
  if (rc != SQLITE_DONE) {
    LOG(ERROR) << "Can't insert data: " << sqlite3_errmsg(db_);
    sqlite3_reset(insert_graph_meta_stmt_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't insert data: " + std::string(sqlite3_errmsg(db_)));
  }
  // get id column
  GraphId graph_id = std::to_string(sqlite3_last_insert_rowid(db_));
  sqlite3_reset(insert_graph_meta_stmt_);
  LOG(INFO) << "Create graph meta successfully: " << graph_id
            << ", payload: " << request.ToString();
  return Result<GraphId>(graph_id);
}

Result<GraphMeta> SqliteMetadataStore::GetGraphMeta(const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_graph_meta_stmt_, 1, graph_id);
  int rc = sqlite3_step(get_graph_meta_stmt_);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(get_graph_meta_stmt_);
    LOG(ERROR) << "Graph not exist: " << graph_id;
    return Status(StatusCode::NotFound, "Graph not exist: " + graph_id);
  }
  GraphMeta graph_meta;
  graph_meta.id = graph_id;
  graph_meta.name = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_graph_meta_stmt_, 1)));
  graph_meta.is_builtin = sqlite3_column_int(get_graph_meta_stmt_, 2);
  graph_meta.description = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_graph_meta_stmt_, 3)));
  graph_meta.creation_time = sqlite3_column_int64(get_graph_meta_stmt_, 4);
  graph_meta.data_update_time = sqlite3_column_int64(get_graph_meta_stmt_, 5);
  auto data_import_config_ptr = reinterpret_cast<const char*>(
      sqlite3_column_text(get_graph_meta_stmt_, 6));
  if (data_import_config_ptr) {
    graph_meta.data_import_config = std::string(data_import_config_ptr);
  }
  graph_meta.graph_schema = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_graph_meta_stmt_, 7)));
  sqlite3_reset(get_graph_meta_stmt_);
  return Result<GraphMeta>(std::move(graph_meta));
}
Result<std::vector<GraphMeta>> SqliteMetadataStore::GetAllGraphMeta() {
  std::vector<GraphMeta> graph_metas;
  int rc = sqlite3_step(get_all_graph_meta_stmt_);
  while (rc == SQLITE_ROW) {
    GraphMeta graph_meta;
    graph_meta.id =
        std::to_string(sqlite3_column_int(get_all_graph_meta_stmt_, 0));
    graph_meta.name = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_graph_meta_stmt_, 1)));
    graph_meta.is_builtin = sqlite3_column_int(get_all_graph_meta_stmt_, 2);
    graph_meta.description = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_graph_meta_stmt_, 3)));
    graph_meta.creation_time =
        sqlite3_column_int64(get_all_graph_meta_stmt_, 4);
    graph_meta.data_update_time =
        sqlite3_column_int64(get_all_graph_meta_stmt_, 5);
    auto data_import_config_ptr = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_graph_meta_stmt_, 6));
    if (data_import_config_ptr) {
      graph_meta.data_import_config = std::string(data_import_config_ptr);
    }
    auto schema_ptr = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_graph_meta_stmt_, 7));
    if (schema_ptr) {
      graph_meta.graph_schema = std::string(schema_ptr);
    } else {
      LOG(INFO) << "graph_schema: nullptr";
    }
    graph_metas.emplace_back(graph_meta);
    rc = sqlite3_step(get_all_graph_meta_stmt_);
  }
  sqlite3_reset(get_all_graph_meta_stmt_);
  return Result<std::vector<GraphMeta>>(std::move(graph_metas));
}
Result<bool> SqliteMetadataStore::DeleteGraphMeta(const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(delete_graph_meta_stmt_, 1, graph_id);
  int rc = sqlite3_step(delete_graph_meta_stmt_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(delete_graph_meta_stmt_);
    LOG(ERROR) << "Can't delete data: " << sqlite3_errmsg(db_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't delete data: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(delete_graph_meta_stmt_);
  return true;
}

// We only update the field that present in update_request.
Result<bool> SqliteMetadataStore::UpdateGraphMeta(
    const GraphId& graph_id, const UpdateGraphMetaRequest& update_request) {
  auto update_graph_meta_sql_str =
      generate_update_graph_meta_sql(graph_id, update_request);
  // run the sql
  return executeSql(update_graph_meta_sql_str);
}

/* Plugin Meta related.
 */
Result<PluginId> SqliteMetadataStore::CreatePluginMeta(
    const CreatePluginMetaRequest& request) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 1,
                                    request.graph_id);  // graph_id
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 2,
                                    request.name);  // name
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 3,
                                    request.description);  // description
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 4,
                                    request.paramsString());  // params
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 5,
                                    request.returnsString());  // returns
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 6,
                                    request.library);  // library path
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 7,
                                    request.optionString());  // option
  SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_plugin_meta_stmt_, 8,
                                       request.enable);  // enable
  int rc = sqlite3_step(insert_plugin_meta_stmt_);
  if (rc != SQLITE_DONE) {
    LOG(ERROR) << "Can't insert data: " << sqlite3_errmsg(db_);
    sqlite3_reset(insert_plugin_meta_stmt_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't insert data: " + std::string(sqlite3_errmsg(db_)));
  }
  PluginId plugin_id = std::to_string(sqlite3_last_insert_rowid(db_));
  sqlite3_reset(insert_plugin_meta_stmt_);
  LOG(INFO) << "Create plugin meta successfully: " << plugin_id
            << ", payload: " << request.ToString();
  return Result<PluginId>(plugin_id);
}
Result<PluginMeta> SqliteMetadataStore::GetPluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_plugin_meta_stmt_, 1, graph_id);
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_plugin_meta_stmt_, 2, plugin_id);
  int rc = sqlite3_step(get_plugin_meta_stmt_);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(get_plugin_meta_stmt_);
    LOG(ERROR) << "Plugin not exists: " << plugin_id;
    return Status(StatusCode::NotFound, "Plugin not exists: " + plugin_id);
  }
  PluginMeta plugin_meta;
  plugin_meta.id = plugin_id;
  plugin_meta.graph_id = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_plugin_meta_stmt_, 1)));
  plugin_meta.name = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_plugin_meta_stmt_, 2)));
  plugin_meta.description = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_plugin_meta_stmt_, 3)));
  plugin_meta.creation_time = sqlite3_column_int64(get_plugin_meta_stmt_, 4);
  plugin_meta.update_time = sqlite3_column_int64(get_plugin_meta_stmt_, 5);
  plugin_meta.setParamsFromJsonString(std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_plugin_meta_stmt_, 6))));
  plugin_meta.setReturnsFromJsonString(
      std::string(reinterpret_cast<const char*>(
          sqlite3_column_text(get_plugin_meta_stmt_, 7))));
  plugin_meta.library = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_plugin_meta_stmt_, 8)));
  plugin_meta.setOptionFromJsonString(std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_plugin_meta_stmt_, 9))));
  plugin_meta.enable = sqlite3_column_int(get_plugin_meta_stmt_, 10);

  sqlite3_reset(get_plugin_meta_stmt_);
  return Result<PluginMeta>(plugin_meta);
}
Result<std::vector<PluginMeta>> SqliteMetadataStore::GetAllPluginMeta(
    const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_all_plugin_meta_stmt_, 1, graph_id);
  std::vector<PluginMeta> plugin_metas;
  int rc = sqlite3_step(get_all_plugin_meta_stmt_);
  while (rc == SQLITE_ROW) {
    PluginMeta plugin_meta;
    plugin_meta.id =
        std::to_string(sqlite3_column_int(get_all_plugin_meta_stmt_, 0));
    plugin_meta.graph_id = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 1)));
    plugin_meta.name = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 2)));
    plugin_meta.description = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 3)));
    plugin_meta.creation_time =
        sqlite3_column_int64(get_all_plugin_meta_stmt_, 4);
    plugin_meta.update_time =
        sqlite3_column_int64(get_all_plugin_meta_stmt_, 5);
    auto params_str = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 6));
    if (params_str) {
      plugin_meta.setParamsFromJsonString(std::string(params_str));
    }
    auto returns_ptr = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 7));
    if (returns_ptr) {
      plugin_meta.setReturnsFromJsonString(std::string(returns_ptr));
    }
    plugin_meta.library = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 8)));
    auto option_ptr = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_plugin_meta_stmt_, 9));
    if (option_ptr) {
      plugin_meta.setOptionFromJsonString(std::string(option_ptr));
    }
    plugin_meta.enable = sqlite3_column_int(get_all_plugin_meta_stmt_, 10);
    plugin_metas.push_back(plugin_meta);
    rc = sqlite3_step(get_all_plugin_meta_stmt_);
  }
  sqlite3_reset(get_all_plugin_meta_stmt_);
  return Result<std::vector<PluginMeta>>(plugin_metas);
}
Result<bool> SqliteMetadataStore::DeletePluginMeta(const GraphId& graph_id,
                                                   const PluginId& plugin_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(delete_plugin_meta_stmt_, 1, plugin_id);
  int rc = sqlite3_step(delete_plugin_meta_stmt_);
  if (rc != SQLITE_DONE) {
    LOG(ERROR) << "Can't delete data: " << sqlite3_errmsg(db_);
    sqlite3_reset(delete_plugin_meta_stmt_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't delete data: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(delete_plugin_meta_stmt_);
  return true;
}

Result<bool> SqliteMetadataStore::DeletePluginMetaByGraphId(
    const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(delete_plugin_meta_graph_id_stmt_, 1,
                                    graph_id);
  int rc = sqlite3_step(delete_plugin_meta_graph_id_stmt_);
  if (rc != SQLITE_DONE) {
    LOG(ERROR) << "Can't delete data: " << sqlite3_errmsg(db_);
    sqlite3_reset(delete_plugin_meta_graph_id_stmt_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't delete data: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(delete_plugin_meta_graph_id_stmt_);
  return true;
}

Result<bool> SqliteMetadataStore::UpdatePluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id,
    const UpdatePluginMetaRequest& update_request) {
  auto update_plugin_meta_sql_str =
      generate_update_plugin_meta_sql(graph_id, plugin_id, update_request);
  return executeSql(update_plugin_meta_sql_str);
}

Result<bool> SqliteMetadataStore::UpdatePluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id,
    const InternalUpdatePluginMetaRequest& update_request) {
  auto update_plugin_meta_sql_str =
      generate_update_plugin_meta_sql(graph_id, plugin_id, update_request);
  return executeSql(update_plugin_meta_sql_str);
}

/*
Job related MetaData.
*/
Result<JobId> SqliteMetadataStore::CreateJobMeta(
    const CreateJobMetaRequest& request) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_job_meta_stmt_, 1, request.graph_id);
  SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_job_meta_stmt_, 2,
                                       request.process_id);  // process_id
  SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(insert_job_meta_stmt_, 3,
                                       request.start_time);  // start_time
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_job_meta_stmt_, 4,
                                    std::to_string(request.status));  // status
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_job_meta_stmt_, 5,
                                    request.log_path);  // log path
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(insert_job_meta_stmt_, 6,
                                    request.type);  // type
  int rc = sqlite3_step(insert_job_meta_stmt_);
  if (rc != SQLITE_DONE) {
    LOG(ERROR) << "Can't insert data: " << sqlite3_errmsg(db_);
    sqlite3_reset(insert_job_meta_stmt_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't insert data: " + std::string(sqlite3_errmsg(db_)));
  }
  JobId job_id = std::to_string(sqlite3_last_insert_rowid(db_));
  sqlite3_reset(insert_job_meta_stmt_);
  LOG(INFO) << "Create job meta successfully: " << job_id
            << ", payload: " << request.ToString();
  return Result<JobId>(job_id);
}
Result<JobMeta> SqliteMetadataStore::GetJobMeta(const JobId& job_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_job_meta_stmt_, 1, job_id);
  int rc = sqlite3_step(get_job_meta_stmt_);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(get_job_meta_stmt_);
    LOG(ERROR) << "Job not exists " << job_id;
    return Status(StatusCode::NotFound, "Job not exists " + job_id);
  }
  JobMeta job_meta;
  job_meta.id = job_id;
  job_meta.graph_id = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_job_meta_stmt_, 1)));
  job_meta.process_id = sqlite3_column_int(get_job_meta_stmt_, 2);
  job_meta.start_time = sqlite3_column_int64(get_job_meta_stmt_, 3);
  job_meta.end_time = sqlite3_column_int64(get_job_meta_stmt_, 4);
  job_meta.status = parseFromString(std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_job_meta_stmt_, 5))));
  job_meta.log_path = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_job_meta_stmt_, 6)));
  job_meta.type = std::string(reinterpret_cast<const char*>(
      sqlite3_column_text(get_job_meta_stmt_, 7)));
  sqlite3_reset(get_job_meta_stmt_);
  return Result<JobMeta>(job_meta);
}
Result<std::vector<JobMeta>> SqliteMetadataStore::GetAllJobMeta() {
  std::vector<JobMeta> job_metas;
  int rc = sqlite3_step(get_all_job_meta_stmt_);
  while (rc == SQLITE_ROW) {
    JobMeta job_meta;
    job_meta.id = std::to_string(sqlite3_column_int(get_all_job_meta_stmt_, 0));
    job_meta.graph_id = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_job_meta_stmt_, 1)));
    job_meta.process_id = sqlite3_column_int(get_all_job_meta_stmt_, 2);
    job_meta.start_time = sqlite3_column_int64(get_all_job_meta_stmt_, 3);
    job_meta.end_time = sqlite3_column_int64(get_all_job_meta_stmt_, 4);
    auto status_ptr = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_job_meta_stmt_, 5));
    if (status_ptr) {
      job_meta.status = parseFromString(std::string(status_ptr));
    }
    auto log_path_ptr = reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_job_meta_stmt_, 6));
    if (log_path_ptr) {
      job_meta.log_path = std::string(log_path_ptr);
    }
    job_meta.type = std::string(reinterpret_cast<const char*>(
        sqlite3_column_text(get_all_job_meta_stmt_, 7)));
    job_metas.emplace_back(job_meta);
    rc = sqlite3_step(get_all_job_meta_stmt_);
  }
  sqlite3_reset(get_all_job_meta_stmt_);
  return Result<std::vector<JobMeta>>(std::move(job_metas));
}
Result<bool> SqliteMetadataStore::DeleteJobMeta(const JobId& job_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(delete_job_meta_stmt_, 1, job_id);
  int rc = sqlite3_step(delete_job_meta_stmt_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(delete_job_meta_stmt_);
    LOG(ERROR) << "Can't delete data: " << sqlite3_errmsg(db_);
    return Status(StatusCode::SQlExecutionError,
                  "Can't delete data: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(delete_job_meta_stmt_);
  return true;
}
Result<bool> SqliteMetadataStore::UpdateJobMeta(
    const JobId& job_id, const UpdateJobMetaRequest& update_request) {
  auto update_job_meta_sql_str =
      generate_update_job_meta_sql(job_id, update_request);
  // run the sql
  return executeSql(update_job_meta_sql_str);
}

Result<bool> SqliteMetadataStore::LockGraphIndices(const GraphId& graph_id) {
  // First get whether the graph indices is locked.
  auto locked_res = GetGraphIndicesLocked(graph_id);
  if (!locked_res.ok()) {
    return locked_res.status();
  }
  if (locked_res.value()) {
    return Status(StatusCode::AlreadyLocked, "Graph indices is already locked");
  }
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(lock_graph_indices_, 1, graph_id);
  int rc = sqlite3_step(lock_graph_indices_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(lock_graph_indices_);
    LOG(ERROR) << "Can't lock graph indices: " << sqlite3_errmsg(db_);
    return Status(
        StatusCode::SQlExecutionError,
        "Can't lock graph indices: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(lock_graph_indices_);
  return true;
}

Result<bool> SqliteMetadataStore::UnlockGraphIndices(const GraphId& graph_id) {
  // First get whether the graph indices is locked.
  auto locked_res = GetGraphIndicesLocked(graph_id);
  if (!locked_res.ok()) {
    return locked_res.status();
  }
  if (!locked_res.value()) {
    return true;  // already unlocked.
  }
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(unlock_graph_indices_, 1, graph_id);
  int rc = sqlite3_step(unlock_graph_indices_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(unlock_graph_indices_);
    LOG(ERROR) << "Can't unlock graph indices: " << sqlite3_errmsg(db_);
    return Status(
        StatusCode::SQlExecutionError,
        "Can't unlock graph indices: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(unlock_graph_indices_);
  return true;
}

Result<bool> SqliteMetadataStore::GetGraphIndicesLocked(
    const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_graph_indices_locked_, 1, graph_id);
  int rc = sqlite3_step(get_graph_indices_locked_);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(get_graph_indices_locked_);
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return Status(StatusCode::NotFound, "Graph not exists: " + graph_id);
  }
  bool locked = sqlite3_column_int(get_graph_indices_locked_, 0);
  sqlite3_reset(get_graph_indices_locked_);
  return locked;
}

Result<bool> SqliteMetadataStore::LockGraphPlugins(const GraphId& graph_id) {
  auto locked_res = GetGraphPluginsLocked(graph_id);
  if (!locked_res.ok()) {
    return locked_res.status();
  }
  if (locked_res.value()) {
    return Status(StatusCode::AlreadyLocked, "Graph plugins is already locked");
  }
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(lock_graph_plugins_, 1, graph_id);
  int rc = sqlite3_step(lock_graph_plugins_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(lock_graph_plugins_);
    LOG(ERROR) << "Can't lock graph plugins: " << sqlite3_errmsg(db_);
    return Status(
        StatusCode::SQlExecutionError,
        "Can't lock graph plugins: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(lock_graph_plugins_);
  return true;
}

Result<bool> SqliteMetadataStore::UnlockGraphPlugins(const GraphId& graph_id) {
  auto locked_res = GetGraphPluginsLocked(graph_id);
  if (!locked_res.ok()) {
    return locked_res.status();
  }
  if (!locked_res.value()) {
    return true;  // already unlocked.
  }
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(unlock_graph_plugins_, 1, graph_id);
  int rc = sqlite3_step(unlock_graph_plugins_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(unlock_graph_plugins_);
    LOG(ERROR) << "Can't unlock graph plugins: " << sqlite3_errmsg(db_);
    return Status(
        StatusCode::SQlExecutionError,
        "Can't unlock graph plugins: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(unlock_graph_plugins_);
  return true;
}

Result<bool> SqliteMetadataStore::GetGraphPluginsLocked(
    const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(get_graph_plugins_locked_, 1, graph_id);
  int rc = sqlite3_step(get_graph_plugins_locked_);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(get_graph_plugins_locked_);
    LOG(ERROR) << "Plugin not exists: " << graph_id;
    return Status(StatusCode::NotFound, "Plugin not exists: " + graph_id);
  }
  bool locked = sqlite3_column_int(get_graph_plugins_locked_, 0);
  sqlite3_reset(get_graph_plugins_locked_);
  return locked;
}

Result<bool> SqliteMetadataStore::SetRunningGraph(const GraphId& graph_id) {
  SQLITE3_BIND_TEXT_OR_ERROR_RETURN(set_running_graph_stmt_, 1, graph_id);
  int rc = sqlite3_step(set_running_graph_stmt_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(set_running_graph_stmt_);
    LOG(ERROR) << "Can't set running graph: " << sqlite3_errmsg(db_);
    return Status(
        StatusCode::SQlExecutionError,
        "Can't set running graph: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(set_running_graph_stmt_);
  return true;
}

Result<GraphId> SqliteMetadataStore::GetRunningGraph() {
  int rc = sqlite3_step(get_running_graph_stmt_);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(get_running_graph_stmt_);
    return Status(StatusCode::NotFound, "No running graph");
  }
  GraphId graph_id =
      std::to_string(sqlite3_column_int(get_running_graph_stmt_, 0));
  sqlite3_reset(get_running_graph_stmt_);
  return Result<GraphId>(graph_id);
}

Result<bool> SqliteMetadataStore::ClearRunningGraph() {
  LOG(INFO) << "Clear running graph";
  int rc = sqlite3_step(clear_running_graph_stmt_);
  if (rc != SQLITE_DONE) {
    sqlite3_reset(clear_running_graph_stmt_);
    LOG(ERROR) << "Can't clear running graph: " << sqlite3_errmsg(db_);
    return Status(
        StatusCode::SQlExecutionError,
        "Can't clear running graph: " + std::string(sqlite3_errmsg(db_)));
  }
  sqlite3_reset(clear_running_graph_stmt_);
  return true;
}

// Private functions.
Result<bool> SqliteMetadataStore::createTables() {
  auto create_graph_table_sql = getCreateGraphTableSql();
  auto create_job_table_sql = getCreateJobTableSql();
  auto create_plugin_table_sql = getCreatePluginTableSql();
  auto create_running_graph_table_sql = getCreateRunningGraphTableSql();

  RETURN_IF_NOT_OK(executeSql(create_graph_table_sql));
  RETURN_IF_NOT_OK(executeSql(create_job_table_sql));
  RETURN_IF_NOT_OK(executeSql(create_plugin_table_sql));
  RETURN_IF_NOT_OK(executeSql(create_running_graph_table_sql));
  return true;
}

Result<bool> SqliteMetadataStore::prepare_statement(const char* sql,
                                                    sqlite3_stmt** stmt) {
  int rc = sqlite3_prepare_v2(db_, sql, -1, stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG(ERROR) << "SQL error: " << sqlite3_errmsg(db_) << ", sql: " << sql;
    return Status(StatusCode::SQlExecutionError,
                  "SQL error: " + std::string(sqlite3_errmsg(db_)));
  }
  return true;
}

Result<bool> SqliteMetadataStore::prepare_statements() {
  RETURN_IF_NOT_OK(
      prepare_statement(INSERT_GRAPH_META, &insert_graph_meta_stmt_));
  RETURN_IF_NOT_OK(prepare_statement(GET_GRAPH_META, &get_graph_meta_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(GET_ALL_GRAPH_META, &get_all_graph_meta_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(DELETE_GRAPH_META, &delete_graph_meta_stmt_));

  RETURN_IF_NOT_OK(
      prepare_statement(INSERT_PLUGIN_META, &insert_plugin_meta_stmt_));
  RETURN_IF_NOT_OK(prepare_statement(GET_PLUGIN_META, &get_plugin_meta_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(GET_ALL_PLUGIN_META, &get_all_plugin_meta_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(DELETE_PLUGIN_META, &delete_plugin_meta_stmt_));
  RETURN_IF_NOT_OK(prepare_statement(DELETE_PLUGIN_META_BY_GRAPH_ID,
                                     &delete_plugin_meta_graph_id_stmt_));

  RETURN_IF_NOT_OK(prepare_statement(INSERT_JOB_META, &insert_job_meta_stmt_));
  RETURN_IF_NOT_OK(prepare_statement(GET_JOB_META, &get_job_meta_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(GET_ALL_JOB_META, &get_all_job_meta_stmt_));
  RETURN_IF_NOT_OK(prepare_statement(DELETE_JOB_META, &delete_job_meta_stmt_));

  RETURN_IF_NOT_OK(prepare_statement(LOCK_GRAPH_INDICES, &lock_graph_indices_));
  RETURN_IF_NOT_OK(
      prepare_statement(UNLOCK_GRAPH_INDICES, &unlock_graph_indices_));
  RETURN_IF_NOT_OK(
      prepare_statement(GET_GRAPH_INDICES_LOCKED, &get_graph_indices_locked_));
  RETURN_IF_NOT_OK(prepare_statement(LOCK_GRAPH_PLUGINS, &lock_graph_plugins_));
  RETURN_IF_NOT_OK(
      prepare_statement(UNLOCK_GRAPH_PLUGINS, &unlock_graph_plugins_));
  RETURN_IF_NOT_OK(
      prepare_statement(GET_GRAPH_PLUGINS_LOCKED, &get_graph_plugins_locked_));
  RETURN_IF_NOT_OK(
      prepare_statement(SET_RUNNING_GRAPH, &set_running_graph_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(GET_RUNNING_GRAPH, &get_running_graph_stmt_));
  RETURN_IF_NOT_OK(
      prepare_statement(CLEAR_RUNNING_GRAPH, &clear_running_graph_stmt_));

  VLOG(10) << "Finish preparing all statements";
  return true;
}

void SqliteMetadataStore::finalize_statements() {
  sqlite3_finalize(insert_graph_meta_stmt_);
  sqlite3_finalize(get_graph_meta_stmt_);
  sqlite3_finalize(get_all_graph_meta_stmt_);
  sqlite3_finalize(delete_graph_meta_stmt_);

  sqlite3_finalize(insert_plugin_meta_stmt_);
  sqlite3_finalize(get_plugin_meta_stmt_);
  sqlite3_finalize(get_all_plugin_meta_stmt_);
  sqlite3_finalize(delete_plugin_meta_stmt_);
  sqlite3_finalize(delete_plugin_meta_graph_id_stmt_);

  sqlite3_finalize(insert_job_meta_stmt_);
  sqlite3_finalize(get_job_meta_stmt_);
  sqlite3_finalize(get_all_job_meta_stmt_);
  sqlite3_finalize(delete_job_meta_stmt_);

  sqlite3_finalize(lock_graph_indices_);
  sqlite3_finalize(unlock_graph_indices_);
  sqlite3_finalize(get_graph_indices_locked_);
  sqlite3_finalize(lock_graph_plugins_);
  sqlite3_finalize(unlock_graph_plugins_);
  sqlite3_finalize(get_graph_plugins_locked_);

  sqlite3_finalize(set_running_graph_stmt_);
  sqlite3_finalize(get_running_graph_stmt_);
  sqlite3_finalize(clear_running_graph_stmt_);
}

std::string SqliteMetadataStore::getCreateGraphTableSql() {
  return getCreateTableSql(GRAPH_META_TABLE_NAME, GRAPH_META_COLUMN_NAMES,
                           GRAPH_META_COLUMN_TYPES,
                           GRAPH_META_COLUMN_IS_PRIMARY_KEY,
                           GRAPH_META_NULLABLE, GRAPH_META_COLUMN_NUM);
}

std::string SqliteMetadataStore::getCreatePluginTableSql() {
  return getCreateTableSql(PLUGIN_META_TABLE_NAME, PLUGIN_META_COLUMN_NAMES,
                           PLUGIN_META_COLUMN_TYPES,
                           PLUGIN_META_COLUMN_IS_PRIMARY_KEY,
                           PLUGIN_META_NULLABLE, PLUGIN_META_COLUMN_NUM);
}

std::string SqliteMetadataStore::getCreateRunningGraphTableSql() {
  return getCreateTableSql(RUNNING_GRAPH_TABLE_NAME, RUNNING_GRAPH_COLUMN_NAMES,
                           RUNNING_GRAPH_COLUMN_TYPES,
                           RUNNING_GRAPH_COLUMN_IS_PRIMARY_KEY,
                           RUNNING_GRAPH_NULLABLE, RUNNING_GRAPH_COLUMN_NUM);
}

std::string SqliteMetadataStore::getCreateJobTableSql() {
  return getCreateTableSql(
      JOB_META_TABLE_NAME, JOB_META_COLUMN_NAMES, JOB_META_COLUMN_TYPES,
      JOB_META_COLUMN_IS_PRIMARY_KEY, JOB_META_NULLABLE, JOB_META_COLUMN_NUM);
}

std::string SqliteMetadataStore::getCreateTableSql(
    const std::string& table_name, const char* const column_names[],
    const char* const column_types[], const bool is_primary_key[],
    const bool nullable[], int32_t column_num) {
  std::stringstream ss;
  ss << "CREATE TABLE IF NOT EXISTS " << table_name << " (";
  for (int32_t i = 0; i < column_num; ++i) {
    ss << column_names[i] << " " << column_types[i];
    if (is_primary_key[i]) {
      ss << " PRIMARY KEY ";
      if (strstr(column_types[i], "INTEGER") != nullptr) {
        ss << " AUTOINCREMENT ";
      }
    }
    // if column_names[i] contains creation, we need to add the default value
    if (strstr(column_names[i], "creation") != nullptr &&
        strcmp(column_types[i], "TIMESTAMP") == 0) {
      ss << " DEFAULT CURRENT_TIMESTAMP";
    }
    if (!nullable[i]) {
      ss << " NOT NULL";
    }
    if (i != column_num - 1) {
      ss << ", ";
    }
  }
  ss << ");";
  auto res = ss.str();
  LOG(INFO) << "Create table sql: " << res;
  return res;
}

Result<bool> SqliteMetadataStore::executeSql(const std::string& sql) {
  char* zErrMsg = 0;
  int rc = sqlite3_exec(db_, sql.c_str(), nullptr, 0, &zErrMsg);
  if (rc != SQLITE_OK) {
    LOG(ERROR) << "SQL error: " << zErrMsg;
    sqlite3_free(zErrMsg);
    return Status(StatusCode::SQlExecutionError,
                  "SQL error: " + std::string(zErrMsg));
  }
  return true;
}

std::string SqliteMetadataStore::generate_update_graph_meta_sql(
    const GraphId& graph_id, const UpdateGraphMetaRequest& request) {
  boost::format fmt(UPDATE_GRAPH_META);

  std::stringstream ss;
  if (!request.graph_name.empty()) {
    ss << ",graph_name = '" << request.graph_name << "'";
  }
  if (!request.description.empty()) {
    ss << ",description = '" << request.description << "'";
  }
  if (request.data_update_time != 0) {
    ss << ",data_update_time = " << request.data_update_time;
  }
  if (!request.data_import_config.empty()) {
    ss << ",data_import_config = '" << request.data_import_config << "'";
  }
  // remove first char is ','
  std::string update_str = ss.str();
  if (!update_str.empty()) {
    update_str = update_str.substr(1);
  }
  fmt % update_str % graph_id;
  return fmt.str();
}

std::string SqliteMetadataStore::generate_update_plugin_meta_sql(
    const GraphId& graph_id, const PluginId& plugin_id,
    const UpdatePluginMetaRequest& request) {
  boost::format fmt(UPDATE_PLUGIN_META);

  std::stringstream ss;
  if (!request.name.empty()) {
    ss << ",plugin_name = '" << request.name << "'";
  }
  if (!request.description.empty()) {
    ss << ",description = '" << request.description << "'";
  }
  if (request.enable.has_value()) {
    ss << ",enable = " << request.enable.value();
  }
  // append current time as update time
  ss << ",update_time = " << std::to_string(GetCurrentTimeStamp());
  // remove first char is ','
  std::string update_str = ss.str();
  if (!update_str.empty()) {
    update_str = update_str.substr(1);
  }
  VLOG(10) << "plugin meta update_str: " << update_str;
  fmt % update_str % plugin_id;
  return fmt.str();
}

std::string SqliteMetadataStore::generate_update_plugin_meta_sql(
    const GraphId& graph_id, const PluginId& plugin_id,
    const InternalUpdatePluginMetaRequest& request) {
  boost::format fmt(UPDATE_PLUGIN_META);

  std::stringstream ss;
  if (!request.name.empty()) {
    ss << ",plugin_name = '" << request.name << "'";
  }
  if (!request.description.empty()) {
    ss << ",description = '" << request.description << "'";
  }
  if (!request.params.empty()) {
    ss << ",params = '" << request.paramsString() << "'";
  }
  if (!request.returns.empty()) {
    ss << ",returns = '" << request.returnsString() << "'";
  }
  if (!request.library.empty()) {
    ss << ",library = '" << request.library << "'";
  }
  if (!request.option.empty()) {
    ss << ",option = '" << request.optionString() << "'";
  }
  if (request.enable.has_value()) {
    ss << ",enable = " << request.enable.value();
  }
  ss << ",update_time = " << std::to_string(GetCurrentTimeStamp());
  // remove first char is ','
  std::string update_str = ss.str();
  if (!update_str.empty()) {
    update_str = update_str.substr(1);
  }
  VLOG(10) << "plugin meta update_str: " << update_str
           << ", plugin id: " << plugin_id;
  try {
    fmt % update_str % plugin_id;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error: " << e.what();
    return "";
  }
  return fmt.str();
}

std::string SqliteMetadataStore::generate_update_job_meta_sql(
    const JobId& job_id, const UpdateJobMetaRequest& request) {
  boost::format fmt(UPDATE_JOB_META);

  std::stringstream ss;
  if (request.status != JobStatus::kUnknown) {
    ss << ",status = '" << request.status << "'";
  }
  if (request.end_time != 0) {
    ss << ",end_time = " << request.end_time;
  }
  // remove first char is ','
  std::string update_str = ss.str();
  if (!update_str.empty()) {
    update_str = update_str.substr(1);
  }
  try {
    fmt % update_str % job_id;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error: " << e.what();
    return "";
  }
  return fmt.str();
}
}  // namespace gs