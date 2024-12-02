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

#include "access/xlog.h"
#include "c.h"
#include "miscadmin.h"
#include "replication/walsender.h"

const char* progname;

void write_pg_loc(const char* dir) {
  // If directory not exists, create it.
  mkdir(dir, 0777);
  // Remove all files under the directory.
  char cmd[1024];
  sprintf(cmd, "rm -rf %s/*", dir);
  system(cmd);
}

int main(int argc, char** argv) {
  // Writer pg_log.
  write_pg_loc("/tmp/wal_testing");
  return 0;
}