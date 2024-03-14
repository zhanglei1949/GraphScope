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

#ifndef FLEX_STORAGES_METADATA_SQLITE_METADATA_STORE_H_
#define FLEX_STORAGES_METADATA_SQLITE_METADATA_STORE_H_

#include <string>
#include <vector>

#include "flex/utils/service_utils.h"

#include <sqlite3.h>
#include <boost/format.hpp>

namespace gs {

// Some utility macro
#define SQLITE3_BIND_TEXT_OR_ERROR_RETURN(stmt, index, text)             \
  if (sqlite3_bind_text(stmt, index, text.c_str(), -1, SQLITE_STATIC) != \
      SQLITE_OK) {                                                       \
    return Status(StatusCode::SqlBindingError,                           \
                  "Failed to bind text to sqlite3 statement, index:" +   \
                      std::to_string(index) + ", text: " + text +        \
                      ", error: " + sqlite3_errmsg(db_));                \
  }

#define SQLITE3_BIND_INTEGER_OR_ERROR_RETURN(stmt, index, integer) \
  if (sqlite3_bind_int64(stmt, index, integer) != SQLITE_OK) {     \
    return Status(StatusCode::SqlBindingError,                     \
                  "Failed to bind integer to sqlite3 statement" +  \
                      std::to_string(integer) +                    \
                      ", error: " + sqlite3_errmsg(db_));          \
  }

#define SQLITE3_BIND_TIMESTAMP_OR_ERROR_RETURN(stmt, index, timestamp) \
  if (sqlite3_bind_int64(stmt, index, timestamp) != SQLITE_OK) {       \
    return Status(StatusCode::SqlBindingError,                         \
                  "Failed to bind timestamp to sqlite3 statement" +    \
                      std::to_string(timestamp) +                      \
                      ", error: " + sqlite3_errmsg(db_));              \
  }

/**
 * @brief SqliteMetadataStore is a concrete implementation of MetadataStore,
 * which stores metadata in sqlite database.
 *
 * By default, we open the database in the serialized mode, which means only
 * one thread can write to the database at a time.
 */
class SqliteMetadataStore : public IMetaDataStore {
 public:
  static constexpr const char* DB_PATH = "/tmp/metadata.db";
  static constexpr const char* GRAPH_META_TABLE_NAME = "GRAPH_META";
  static constexpr const char* PLUGIN_META_TABLE_NAME = "PLUGIN_META";
  static constexpr const char* JOB_META_TABLE_NAME = "JOB_META";
  static constexpr const char* RUNNING_GRAPH_TABLE_NAME = "RUNNING_GRAPH";

  /////////////////////////// Graph Meta related ///////////////////////////
  static constexpr int32_t GRAPH_META_COLUMN_NUM = 10;
  // a list of GRAPH_META column names
  static constexpr const char* GRAPH_META_COLUMN_NAMES[GRAPH_META_COLUMN_NUM] =
      {"graph_id",           "graph_name",    "is_builtin",
       "description",        "creation_time", "data_update_time",
       "data_import_config", "graph_schema",  "indicies_locked",
       "plugins_locked"};
  // Integer is 64 bit in sqlite3
  // sqlite doesn't have a boolean type, we use integer instead.
  static constexpr const char* GRAPH_META_COLUMN_TYPES[GRAPH_META_COLUMN_NUM] =
      {"INTEGER", "TEXT", "INTEGER", "TEXT",    "INTEGER",
       "INTEGER", "TEXT", "TEXT",    "INTEGER", "INTEGER"};
  static constexpr bool
      GRAPH_META_COLUMN_IS_PRIMARY_KEY[GRAPH_META_COLUMN_NUM] = {
          true, false, false, false, false, false, false, false, false, false};
  static constexpr bool GRAPH_META_NULLABLE[GRAPH_META_COLUMN_NUM] = {
      false, false, false, true, false, true, true, false, false, false};

  /////////////////////////// Plugin Meta related ///////////////////////////
  static constexpr int32_t PLUGIN_META_COLUMN_NUM = 11;
  // a list of PLUGIN_META column names
  static constexpr const char*
      PLUGIN_META_COLUMN_NAMES[PLUGIN_META_COLUMN_NUM] = {
          "plugin_id",     "graph_id",    "plugin_name", "description",
          "creation_time", "update_time", "params",      "returns",
          "library",       "option",      "enable"};
  static constexpr const char*
      PLUGIN_META_COLUMN_TYPES[PLUGIN_META_COLUMN_NUM] = {
          "INTEGER", "TEXT", "TEXT", "TEXT", "INTEGER", "INTEGER",
          "TEXT",    "TEXT", "TEXT", "TEXT", "INTEGER"};
  static constexpr bool
      PLUGIN_META_COLUMN_IS_PRIMARY_KEY[PLUGIN_META_COLUMN_NUM] = {
          true,  false, false, false, false, false,
          false, false, false, false, false};
  static constexpr bool PLUGIN_META_NULLABLE[PLUGIN_META_COLUMN_NUM] = {
      false, false, false, true, false, true, false, false, false, true, false};

  /////////////////////////// Job Meta related ///////////////////////////
  static constexpr int32_t JOB_META_COLUMN_NUM = 8;
  // a list of JOB_META column names
  static constexpr const char* JOB_META_COLUMN_NAMES[JOB_META_COLUMN_NUM] = {
      "job_id",   "graph_id", "process_id", "start_time",
      "end_time", "status",   "log_path",   "type"};
  static constexpr const char* JOB_META_COLUMN_TYPES[JOB_META_COLUMN_NUM] = {
      "INTEGER", "TEXT", "INTEGER", "INTEGER",
      "INTEGER", "TEXT", "TEXT",    "TEXT"};
  static constexpr bool JOB_META_COLUMN_IS_PRIMARY_KEY[JOB_META_COLUMN_NUM] = {
      true, false, false, false, false, false, false, false};
  static constexpr bool JOB_META_NULLABLE[JOB_META_COLUMN_NUM] = {
      false, false, false, false, true, false, true, false};

  /////////////////////////// Running Graph related ///////////////////////////
  static constexpr int32_t RUNNING_GRAPH_COLUMN_NUM = 2;
  // a list of RUNNING_GRAPH column names
  static constexpr const char*
      RUNNING_GRAPH_COLUMN_NAMES[RUNNING_GRAPH_COLUMN_NUM] = {"status",
                                                              "graph_id"};
  static constexpr const char*
      RUNNING_GRAPH_COLUMN_TYPES[RUNNING_GRAPH_COLUMN_NUM] = {"TEXT", "TEXT"};
  static constexpr bool
      RUNNING_GRAPH_COLUMN_IS_PRIMARY_KEY[RUNNING_GRAPH_COLUMN_NUM] = {true,
                                                                       false};
  static constexpr bool RUNNING_GRAPH_NULLABLE[RUNNING_GRAPH_COLUMN_NUM] = {
      false, false};

  /////////////////////////// Prepared statements ///////////////////////////
  // The table name can not be parameterized
  static constexpr const char* INSERT_GRAPH_META =
      "INSERT INTO GRAPH_META (graph_name, is_builtin, description, "
      "creation_time, data_update_time, graph_schema, indicies_locked, "
      "plugins_locked"
      ") VALUES (:graph_name, :is_builtin, :description, "
      "strftime('%s', 'now') * 1000 + CAST(strftime('%f','now') AS INTEGER) / "
      "1000, :data_update_time, :graph_schema, :indicies_locked, "
      ":plugins_locked)";
  static constexpr const char* GET_GRAPH_META =
      "SELECT * FROM GRAPH_META WHERE graph_id = :graph_id";

  static constexpr const char* GET_ALL_GRAPH_META = "SELECT * FROM GRAPH_META";
  // prepare a transaction, delete from both GRAPH_META and PLUGIN_META
  static constexpr const char* DELETE_GRAPH_META =
      "DELETE FROM GRAPH_META WHERE graph_id = :graph_id";

  static constexpr const char* UPDATE_GRAPH_META =
      "UPDATE GRAPH_META SET %1% WHERE graph_id = %2%";

  static constexpr const char* INSERT_PLUGIN_META =
      "INSERT INTO PLUGIN_META (graph_id, plugin_name,  description, "
      "creation_time, params, returns, library, option, enable) VALUES "
      "(:graph_id, "
      ":plugin_name, "
      ":description, strftime('%s', 'now') * 1000 + CAST(strftime('%f','now') "
      "AS INTEGER) / 1000,"
      " :params, :returns, :library, :option, :enable)";
  static constexpr const char* GET_PLUGIN_META =
      "SELECT * FROM PLUGIN_META WHERE graph_id = :graph_id AND "
      "plugin_id = :plugin_id";
  static constexpr const char* GET_ALL_PLUGIN_META =
      "SELECT * FROM PLUGIN_META WHERE graph_id = :graph_id";
  static constexpr const char* DELETE_PLUGIN_META =
      "DELETE FROM PLUGIN_META WHERE "
      "plugin_id = :plugin_id";
  static constexpr const char* DELETE_PLUGIN_META_BY_GRAPH_ID =
      "DELETE FROM PLUGIN_META WHERE graph_id = :graph_id";
  static constexpr const char* UPDATE_PLUGIN_META =
      "UPDATE PLUGIN_META SET %1% WHERE plugin_id = %2%";

  static constexpr const char* INSERT_JOB_META =
      "INSERT INTO JOB_META (graph_id, process_id, start_time, status, "
      "log_path, type) "
      "VALUES (:graph_id, :process_id, :start_time, :status, "
      ":log_path, :type)";

  static constexpr const char* GET_JOB_META =
      "SELECT * FROM JOB_META WHERE job_id = :job_id";
  static constexpr const char* GET_ALL_JOB_META = "SELECT * FROM JOB_META";
  static constexpr const char* DELETE_JOB_META =
      "DELETE FROM JOB_META WHERE job_id = :job_id";
  static constexpr const char* UPDATE_JOB_META =
      "UPDATE JOB_META SET %1% "
      " WHERE job_id = %2%";

  static constexpr const char* LOCK_GRAPH_INDICES =
      "UPDATE GRAPH_META SET indicies_locked = 1 WHERE graph_id = :graph_id";
  static constexpr const char* UNLOCK_GRAPH_INDICES =
      "UPDATE GRAPH_META SET indicies_locked = 0 WHERE graph_id = :graph_id";
  static constexpr const char* GET_GRAPH_INDICES_LOCKED =
      "SELECT indicies_locked FROM GRAPH_META WHERE graph_id = :graph_id";

  static constexpr const char* LOCK_GRAPH_PLUGINS =
      "UPDATE GRAPH_META SET plugins_locked = 1 WHERE graph_id = :graph_id";
  static constexpr const char* UNLOCK_GRAPH_PLUGINS =
      "UPDATE GRAPH_META SET plugins_locked = 0 WHERE graph_id = :graph_id";
  static constexpr const char* GET_GRAPH_PLUGINS_LOCKED =
      "SELECT plugins_locked FROM GRAPH_META WHERE graph_id = :graph_id";

  /////////////////////////// Running Graph related ///////////////////////////
  static constexpr const char* SET_RUNNING_GRAPH =
      "INSERT OR REPLACE INTO RUNNING_GRAPH (status, graph_id) VALUES "
      "('running', "
      ":graph_id)";
  static constexpr const char* GET_RUNNING_GRAPH =
      "SELECT graph_id FROM RUNNING_GRAPH where status == 'running'";
  static constexpr const char* CLEAR_RUNNING_GRAPH =
      "DELETE FROM RUNNING_GRAPH";

  SqliteMetadataStore();

  ~SqliteMetadataStore();

  Result<bool> Open() override;

  Result<bool> Close() override;
  /* Graph Meta related.
   */
  Result<GraphId> CreateGraphMeta(
      const CreateGraphMetaRequest& request) override;
  Result<GraphMeta> GetGraphMeta(const GraphId& graph_id) override;
  Result<std::vector<GraphMeta>> GetAllGraphMeta() override;
  Result<bool> DeleteGraphMeta(const GraphId& graph_id) override;
  Result<bool> UpdateGraphMeta(
      const GraphId& graph_id,
      const UpdateGraphMetaRequest& update_request) override;
  /* Plugin Meta related.
   */
  Result<PluginId> CreatePluginMeta(
      const CreatePluginMetaRequest& request) override;
  Result<PluginMeta> GetPluginMeta(const GraphId& graph_id,
                                   const PluginId& plugin_id) override;
  Result<std::vector<PluginMeta>> GetAllPluginMeta(
      const GraphId& graph_id) override;
  Result<bool> DeletePluginMeta(const GraphId& graph_id,
                                const PluginId& plugin_id) override;
  Result<bool> DeletePluginMetaByGraphId(const GraphId& graph_id) override;
  Result<bool> UpdatePluginMeta(
      const GraphId& graph_id, const PluginId& plugin_id,
      const UpdatePluginMetaRequest& update_request) override;

  Result<bool> UpdatePluginMeta(
      const GraphId& graph_id, const PluginId& plugin_id,
      const InternalUpdatePluginMetaRequest& update_request) override;

  /*
  Job related MetaData.
  */
  Result<JobId> CreateJobMeta(const CreateJobMetaRequest& request) override;
  Result<JobMeta> GetJobMeta(const JobId& job_id) override;
  Result<std::vector<JobMeta>> GetAllJobMeta() override;
  Result<bool> DeleteJobMeta(const JobId& job_id) override;
  Result<bool> UpdateJobMeta(
      const JobId& job_id, const UpdateJobMetaRequest& update_request) override;

  /*Lock graph and unlock graph
   */
  Result<bool> LockGraphIndices(const GraphId& graph_id) override;
  Result<bool> UnlockGraphIndices(const GraphId& graph_id) override;
  Result<bool> GetGraphIndicesLocked(const GraphId& graph_id) override;

  Result<bool> LockGraphPlugins(const GraphId& graph_id) override;
  Result<bool> UnlockGraphPlugins(const GraphId& graph_id) override;
  Result<bool> GetGraphPluginsLocked(const GraphId& graph_id) override;

  Result<bool> SetRunningGraph(const GraphId& graph_id) override;
  Result<GraphId> GetRunningGraph() override;
  Result<bool> ClearRunningGraph() override;

 private:
  Result<bool> createTables();
  Result<bool> prepare_statement(const char* sql, sqlite3_stmt** stmt);
  Result<bool> prepare_statements();
  void finalize_statements();
  std::string getCreateGraphTableSql();
  std::string getCreatePluginTableSql();
  std::string getCreateJobTableSql();
  std::string getCreateRunningGraphTableSql();

  std::string getCreateTableSql(const std::string& table_name,
                                const char* const column_names[],
                                const char* const column_types[],
                                const bool is_primary_key[],
                                const bool nullable[], int32_t column_num);

  /*
    * Execute a sql statement.
    * @param sql The sql statement to execute.
    * @return True if the sql statement is executed successfully, false
     otherwise.
  */
  Result<bool> executeSql(const std::string& sql);
  Result<bool> executeStmt(sqlite3_stmt* stmt);

  std::string generate_update_graph_meta_sql(
      const GraphId& graph_id, const UpdateGraphMetaRequest& update_request);
  std::string generate_update_plugin_meta_sql(
      const GraphId& graph_id, const PluginId& plugin_id,
      const UpdatePluginMetaRequest& update_request);
  std::string generate_update_plugin_meta_sql(
      const GraphId& graph_id, const PluginId& plugin_id,
      const InternalUpdatePluginMetaRequest& update_request);
  std::string generate_update_job_meta_sql(
      const JobId& job_id, const UpdateJobMetaRequest& update_request);

  sqlite3* db_;

  sqlite3_stmt* insert_graph_meta_stmt_;
  sqlite3_stmt* get_graph_meta_stmt_;
  sqlite3_stmt* get_all_graph_meta_stmt_;
  sqlite3_stmt* delete_graph_meta_stmt_;

  sqlite3_stmt* insert_plugin_meta_stmt_;
  sqlite3_stmt* get_plugin_meta_stmt_;
  sqlite3_stmt* get_all_plugin_meta_stmt_;
  sqlite3_stmt* delete_plugin_meta_stmt_;
  sqlite3_stmt* delete_plugin_meta_graph_id_stmt_;

  sqlite3_stmt* insert_job_meta_stmt_;
  sqlite3_stmt* get_job_meta_stmt_;
  sqlite3_stmt* get_all_job_meta_stmt_;
  sqlite3_stmt* delete_job_meta_stmt_;

  sqlite3_stmt* lock_graph_indices_stmt_;
  sqlite3_stmt* unlock_graph_indices_stmt_;
  sqlite3_stmt* lock_graph_plugins_stmt_;
  sqlite3_stmt* unlock_graph_plugins_stmt_;

  sqlite3_stmt* lock_graph_indices_;
  sqlite3_stmt* unlock_graph_indices_;
  sqlite3_stmt* get_graph_indices_locked_;

  sqlite3_stmt* lock_graph_plugins_;
  sqlite3_stmt* unlock_graph_plugins_;
  sqlite3_stmt* get_graph_plugins_locked_;

  sqlite3_stmt* set_running_graph_stmt_;
  sqlite3_stmt* get_running_graph_stmt_;
  sqlite3_stmt* clear_running_graph_stmt_;
};
}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_SQLITE_METADATA_STORE_H_