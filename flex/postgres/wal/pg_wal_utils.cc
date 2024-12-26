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
#include "flex/postgres/wal/pg_wal_utils.h"
// #include "flex/postgres/wal/pg/write_wal.h"

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

// #include <glog/logging.h>
#include <stdlib.h>

#include "postgres.h"
#include "utils/elog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"

std::string get_current_prog_dir() {
  // Get the directory of the current program.
  // The program path is stored in the /proc/self/exe.
  // Read the /proc/self/exe to get the program path.
  // The program path is the directory of the program.
  // The program path is the directory of the program.
  auto path = std::filesystem::read_symlink("/proc/self/exe");
  return path.parent_path().string();
}

std::string find_flex_initdb_binary() {
  // Find the via the relative path: ../adaptor/bin/flex_initdb
  // First get current binary directory
  // Then append the relative path to the binary directory.
  std::string cur_prog_path = get_current_prog_dir();
  //   ereport(LOG, (errmsg("Current program path: %s", cur_prog_path)));
  // printf("Current program path: %s", cur_prog_path);
  // LOG(INFO) << "Current program path: " << cur_prog_path;

  // snprintf(flex_initdb_path, 1024, "%s/../adaptor/bin/flex_initdb",
  //  cur_prog_path);
  //   ereport(LOG, (errmsg("Flex initdb path: %s", flex_initdb_path)));
  //   LOG(INFO) << "Flex initdb path: " << flex_initdb_path;
  // printf("Flex initdb path: %s", flex_initdb_path);
  // LOG(INFO) << "Flex initdb path: " << flex_initdb_path;
  std::string flex_initdb_path =
      std::string(cur_prog_path) + "/../adaptor/bin/flex_initdb";
  std::cout<< "flex_initdb_path: " << flex_initdb_path << std::endl;
  // free(cur_prog_path);
  return flex_initdb_path;
  // TODO: Support install path
}

void initialize_database(const std::string& path) {
  // If the directory already exists, just return.
  if (std::filesystem::exists(path)) {
    // check whether the directory is empty
    if (!std::filesystem::is_empty(path)) {
      std::cout << "The directory already exists and is not empty."
                << std::endl;
      // check postgresql.conf exists
      std::string postgresql_conf = path + "/postgresql.conf";
      if (std::filesystem::exists(postgresql_conf)) {
        std::cout << "Skip the initialization." << std::endl;
        return;
      } else {
        std::cerr << "The directory " << path
                  << " already exists but does not contain postgresql.conf."
                  << std::endl;
        exit(1);
      }
    }
  }
  // find flex_initdb binary
  std::string flex_initdb_path = find_flex_initdb_binary();
  // initialize the database
  std::string cmd = flex_initdb_path + " -D " + path;
  std::cout << "Initialize the database with command: " << cmd << std::endl;
  int ret = system(cmd.c_str());
  if (ret != 0) {
    std::cout << "Failed to initialize the database." << std::endl;
  }
  std::cout << "Successfully initialized the database." << std::endl;
}

void StartPostMaster(const std::string& path) {
  // Start the postmaster in this process to handle the wal writing. It will
  // create several subprocesses.
  //
  // 1. WalWriter: It will write the wal to the disk.
  //
  // The wal will be written to the directory specified by the path_.
  // In this function, we will do two things:
  // 1. First findout the flex_initdb binary path, and use it to initialize the
  // db directory.
  //    The postgres database directory may contains some files that are not
  //    needed by flex.
  // 2. Use this binary to initialize the path directory.
  // 3. Start the postmaster in this process.
  std::cout << "initialize_database" << std::endl;
  ereport(LOG, (errmsg("initialize_database")));
  initialize_database(path);
  XLogRecPtr ptr = GetInsertRecPtr();
  std::cout << "The last wal record ptr is: " << ptr << std::endl;
  // initialize_postmaster(path.c_str());
  //(TODO) Use boost::subprocess to start the postmaster.
}


bool WriteWal(const char* data, size_t length) {
  ereport(LOG, (errmsg("Before XLogBeginInsert")));
	XLogBeginInsert();
  ereport(LOG, (errmsg("Before XLogRegisterData")));
	XLogRegisterData(data, length);
  ereport(LOG, (errmsg("Before XLogInsert")));
	XLogRecPtr	recptr = XLogInsert(RM_XLOG_ID, XLOG_FLEX_WAL_REDO);
  ereport(LOG, (errmsg("After XLogInsert")));
	XLogFlush(recptr);
  ereport(LOG, (errmsg("After XLogFlush")));
  return true;
}