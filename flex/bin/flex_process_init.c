
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogarchive.h"
#include "postmaster/auxprocess.h"
#include "storage/proc.h"
#include "utils/elog.h"

#include "c.h"
#include "common/fe_memutils.h"
#include "miscadmin.h"
#include "postgres.h"

#include "flex/bin/flex_process_init.h"

void FLexMainInit() {
  /// Initialize the xlog required.
  MyBackendType = B_FLEX;
  ereport(
      LOG,
      (errmsg("<<<<<<<<<<<<<<<<before auxilary ProcessMain common..., pid %d",
              getpid())));
  AuxiliaryProcessMainCommon();
  ereport(
      LOG,
      (errmsg("<<<<<<<<<<<<<<<<after auxilary ProcessMain common..., pid %d",
              getpid())));
  // on_shmem_exit(StartupProcExit, 0);
  ereport(LOG,
          (errmsg("<<<<<<<<<<<<<<<<before StartXlog..., pid %d", getpid())));
// InitProcess();
  BaseInit();
  StartupXLOG();

  ereport(LOG,
          (errmsg("<<<<<<<<<<<<<<<<after StartXlog..., pid %d", getpid())));
//   InitPostgres(NULL, InvalidOid, NULL, InvalidOid, 0, NULL);
  ereport(LOG,
          (errmsg("<<<<<<<<<<<<<<<<after InitPostgres..., pid %d", getpid())));
}