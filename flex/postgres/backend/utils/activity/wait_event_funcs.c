/*------------------------------------------------------------------------
 *
 * wait_event_funcs.c
 *	  Functions for accessing wait event data.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/wait_event_funcs.c
 *
 *------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"

/*
 * Each wait event has one corresponding entry in this structure, fed to
 * the SQL function of this file.
 */
static const struct
{
	const char *type;
	const char *name;
	const char *description;
}

			waitEventData[] =
{

	{"Activity", "ArchiverMain", "Waiting in main loop of archiver process"},
	{"Activity", "AutovacuumMain", "Waiting in main loop of autovacuum launcher process"},
	{"Activity", "BgwriterHibernate", "Waiting in background writer process, hibernating"},
	{"Activity", "BgwriterMain", "Waiting in main loop of background writer process"},
	{"Activity", "CheckpointerMain", "Waiting in main loop of checkpointer process"},
	{"Activity", "LogicalApplyMain", "Waiting in main loop of logical replication apply process"},
	{"Activity", "LogicalLauncherMain", "Waiting in main loop of logical replication launcher process"},
	{"Activity", "LogicalParallelApplyMain", "Waiting in main loop of logical replication parallel apply process"},
	{"Activity", "RecoveryWalStream", "Waiting in main loop of startup process for WAL to arrive, during streaming recovery"},
	{"Activity", "ReplicationSlotsyncMain", "Waiting in main loop of slot sync worker"},
	{"Activity", "ReplicationSlotsyncShutdown", "Waiting for slot sync worker to shut down"},
	{"Activity", "SysloggerMain", "Waiting in main loop of syslogger process"},
	{"Activity", "WalReceiverMain", "Waiting in main loop of WAL receiver process"},
	{"Activity", "WalSenderMain", "Waiting in main loop of WAL sender process"},
	{"Activity", "WalSummarizerWal", "Waiting in WAL summarizer for more WAL to be generated"},
	{"Activity", "WalWriterMain", "Waiting in main loop of WAL writer process"},
	{"BufferPin", "BufferPin", "Waiting to acquire an exclusive pin on a buffer"},
	{"Client", "ClientRead", "Waiting to read data from the client"},
	{"Client", "ClientWrite", "Waiting to write data to the client"},
	{"Client", "GssOpenServer", "Waiting to read data from the client while establishing a GSSAPI session"},
	{"Client", "LibpqwalreceiverConnect", "Waiting in WAL receiver to establish connection to remote server"},
	{"Client", "LibpqwalreceiverReceive", "Waiting in WAL receiver to receive data from remote server"},
	{"Client", "SslOpenServer", "Waiting for SSL while attempting connection"},
	{"Client", "WaitForStandbyConfirmation", "Waiting for WAL to be received and flushed by the physical standby"},
	{"Client", "WalSenderWaitForWal", "Waiting for WAL to be flushed in WAL sender process"},
	{"Client", "WalSenderWriteData", "Waiting for any activity when processing replies from WAL receiver in WAL sender process"},
	{"Extension", "Extension", "Waiting in an extension"},
	{"IO", "BasebackupRead", "Waiting for base backup to read from a file"},
	{"IO", "BasebackupSync", "Waiting for data written by a base backup to reach durable storage"},
	{"IO", "BasebackupWrite", "Waiting for base backup to write to a file"},
	{"IO", "BuffileRead", "Waiting for a read from a buffered file"},
	{"IO", "BuffileTruncate", "Waiting for a buffered file to be truncated"},
	{"IO", "BuffileWrite", "Waiting for a write to a buffered file"},
	{"IO", "ControlFileRead", "Waiting for a read from the pg_control file"},
	{"IO", "ControlFileSync", "Waiting for the pg_control file to reach durable storage"},
	{"IO", "ControlFileSyncUpdate", "Waiting for an update to the pg_control file to reach durable storage"},
	{"IO", "ControlFileWrite", "Waiting for a write to the pg_control file"},
	{"IO", "ControlFileWriteUpdate", "Waiting for a write to update the pg_control file"},
	{"IO", "CopyFileRead", "Waiting for a read during a file copy operation"},
	{"IO", "CopyFileWrite", "Waiting for a write during a file copy operation"},
	{"IO", "DataFileExtend", "Waiting for a relation data file to be extended"},
	{"IO", "DataFileFlush", "Waiting for a relation data file to reach durable storage"},
	{"IO", "DataFileImmediateSync", "Waiting for an immediate synchronization of a relation data file to durable storage"},
	{"IO", "DataFilePrefetch", "Waiting for an asynchronous prefetch from a relation data file"},
	{"IO", "DataFileRead", "Waiting for a read from a relation data file"},
	{"IO", "DataFileSync", "Waiting for changes to a relation data file to reach durable storage"},
	{"IO", "DataFileTruncate", "Waiting for a relation data file to be truncated"},
	{"IO", "DataFileWrite", "Waiting for a write to a relation data file"},
	{"IO", "DsmAllocate", "Waiting for a dynamic shared memory segment to be allocated"},
	{"IO", "DsmFillZeroWrite", "Waiting to fill a dynamic shared memory backing file with zeroes"},
	{"IO", "LockFileAddtodatadirRead", "Waiting for a read while adding a line to the data directory lock file"},
	{"IO", "LockFileAddtodatadirSync", "Waiting for data to reach durable storage while adding a line to the data directory lock file"},
	{"IO", "LockFileAddtodatadirWrite", "Waiting for a write while adding a line to the data directory lock file"},
	{"IO", "LockFileCreateRead", "Waiting to read while creating the data directory lock file"},
	{"IO", "LockFileCreateSync", "Waiting for data to reach durable storage while creating the data directory lock file"},
	{"IO", "LockFileCreateWrite", "Waiting for a write while creating the data directory lock file"},
	{"IO", "LockFileRecheckdatadirRead", "Waiting for a read during recheck of the data directory lock file"},
	{"IO", "LogicalRewriteCheckpointSync", "Waiting for logical rewrite mappings to reach durable storage during a checkpoint"},
	{"IO", "LogicalRewriteMappingSync", "Waiting for mapping data to reach durable storage during a logical rewrite"},
	{"IO", "LogicalRewriteMappingWrite", "Waiting for a write of mapping data during a logical rewrite"},
	{"IO", "LogicalRewriteSync", "Waiting for logical rewrite mappings to reach durable storage"},
	{"IO", "LogicalRewriteTruncate", "Waiting for truncate of mapping data during a logical rewrite"},
	{"IO", "LogicalRewriteWrite", "Waiting for a write of logical rewrite mappings"},
	{"IO", "RelationMapRead", "Waiting for a read of the relation map file"},
	{"IO", "RelationMapReplace", "Waiting for durable replacement of a relation map file"},
	{"IO", "RelationMapWrite", "Waiting for a write to the relation map file"},
	{"IO", "ReorderBufferRead", "Waiting for a read during reorder buffer management"},
	{"IO", "ReorderBufferWrite", "Waiting for a write during reorder buffer management"},
	{"IO", "ReorderLogicalMappingRead", "Waiting for a read of a logical mapping during reorder buffer management"},
	{"IO", "ReplicationSlotRead", "Waiting for a read from a replication slot control file"},
	{"IO", "ReplicationSlotRestoreSync", "Waiting for a replication slot control file to reach durable storage while restoring it to memory"},
	{"IO", "ReplicationSlotSync", "Waiting for a replication slot control file to reach durable storage"},
	{"IO", "ReplicationSlotWrite", "Waiting for a write to a replication slot control file"},
	{"IO", "SlruFlushSync", "Waiting for SLRU data to reach durable storage during a checkpoint or database shutdown"},
	{"IO", "SlruRead", "Waiting for a read of an SLRU page"},
	{"IO", "SlruSync", "Waiting for SLRU data to reach durable storage following a page write"},
	{"IO", "SlruWrite", "Waiting for a write of an SLRU page"},
	{"IO", "SnapbuildRead", "Waiting for a read of a serialized historical catalog snapshot"},
	{"IO", "SnapbuildSync", "Waiting for a serialized historical catalog snapshot to reach durable storage"},
	{"IO", "SnapbuildWrite", "Waiting for a write of a serialized historical catalog snapshot"},
	{"IO", "TimelineHistoryFileSync", "Waiting for a timeline history file received via streaming replication to reach durable storage"},
	{"IO", "TimelineHistoryFileWrite", "Waiting for a write of a timeline history file received via streaming replication"},
	{"IO", "TimelineHistoryRead", "Waiting for a read of a timeline history file"},
	{"IO", "TimelineHistorySync", "Waiting for a newly created timeline history file to reach durable storage"},
	{"IO", "TimelineHistoryWrite", "Waiting for a write of a newly created timeline history file"},
	{"IO", "TwophaseFileRead", "Waiting for a read of a two phase state file"},
	{"IO", "TwophaseFileSync", "Waiting for a two phase state file to reach durable storage"},
	{"IO", "TwophaseFileWrite", "Waiting for a write of a two phase state file"},
	{"IO", "VersionFileSync", "Waiting for the version file to reach durable storage while creating a database"},
	{"IO", "VersionFileWrite", "Waiting for the version file to be written while creating a database"},
	{"IO", "WalsenderTimelineHistoryRead", "Waiting for a read from a timeline history file during a walsender timeline command"},
	{"IO", "WalBootstrapSync", "Waiting for WAL to reach durable storage during bootstrapping"},
	{"IO", "WalBootstrapWrite", "Waiting for a write of a WAL page during bootstrapping"},
	{"IO", "WalCopyRead", "Waiting for a read when creating a new WAL segment by copying an existing one"},
	{"IO", "WalCopySync", "Waiting for a new WAL segment created by copying an existing one to reach durable storage"},
	{"IO", "WalCopyWrite", "Waiting for a write when creating a new WAL segment by copying an existing one"},
	{"IO", "WalInitSync", "Waiting for a newly initialized WAL file to reach durable storage"},
	{"IO", "WalInitWrite", "Waiting for a write while initializing a new WAL file"},
	{"IO", "WalRead", "Waiting for a read from a WAL file"},
	{"IO", "WalSummaryRead", "Waiting for a read from a WAL summary file"},
	{"IO", "WalSummaryWrite", "Waiting for a write to a WAL summary file"},
	{"IO", "WalSync", "Waiting for a WAL file to reach durable storage"},
	{"IO", "WalSyncMethodAssign", "Waiting for data to reach durable storage while assigning a new WAL sync method"},
	{"IO", "WalWrite", "Waiting for a write to a WAL file"},
	{"IPC", "AppendReady", "Waiting for subplan nodes of an Append plan node to be ready"},
	{"IPC", "ArchiveCleanupCommand", "Waiting for archive_cleanup_command to complete"},
	{"IPC", "ArchiveCommand", "Waiting for archive_command to complete"},
	{"IPC", "BackendTermination", "Waiting for the termination of another backend"},
	{"IPC", "BackupWaitWalArchive", "Waiting for WAL files required for a backup to be successfully archived"},
	{"IPC", "BgworkerShutdown", "Waiting for background worker to shut down"},
	{"IPC", "BgworkerStartup", "Waiting for background worker to start up"},
	{"IPC", "BtreePage", "Waiting for the page number needed to continue a parallel B-tree scan to become available"},
	{"IPC", "BufferIo", "Waiting for buffer I/O to complete"},
	{"IPC", "CheckpointDelayComplete", "Waiting for a backend that blocks a checkpoint from completing"},
	{"IPC", "CheckpointDelayStart", "Waiting for a backend that blocks a checkpoint from starting"},
	{"IPC", "CheckpointDone", "Waiting for a checkpoint to complete"},
	{"IPC", "CheckpointStart", "Waiting for a checkpoint to start"},
	{"IPC", "ExecuteGather", "Waiting for activity from a child process while executing a Gather plan node"},
	{"IPC", "HashBatchAllocate", "Waiting for an elected Parallel Hash participant to allocate a hash table"},
	{"IPC", "HashBatchElect", "Waiting to elect a Parallel Hash participant to allocate a hash table"},
	{"IPC", "HashBatchLoad", "Waiting for other Parallel Hash participants to finish loading a hash table"},
	{"IPC", "HashBuildAllocate", "Waiting for an elected Parallel Hash participant to allocate the initial hash table"},
	{"IPC", "HashBuildElect", "Waiting to elect a Parallel Hash participant to allocate the initial hash table"},
	{"IPC", "HashBuildHashInner", "Waiting for other Parallel Hash participants to finish hashing the inner relation"},
	{"IPC", "HashBuildHashOuter", "Waiting for other Parallel Hash participants to finish partitioning the outer relation"},
	{"IPC", "HashGrowBatchesDecide", "Waiting to elect a Parallel Hash participant to decide on future batch growth"},
	{"IPC", "HashGrowBatchesElect", "Waiting to elect a Parallel Hash participant to allocate more batches"},
	{"IPC", "HashGrowBatchesFinish", "Waiting for an elected Parallel Hash participant to decide on future batch growth"},
	{"IPC", "HashGrowBatchesReallocate", "Waiting for an elected Parallel Hash participant to allocate more batches"},
	{"IPC", "HashGrowBatchesRepartition", "Waiting for other Parallel Hash participants to finish repartitioning"},
	{"IPC", "HashGrowBucketsElect", "Waiting to elect a Parallel Hash participant to allocate more buckets"},
	{"IPC", "HashGrowBucketsReallocate", "Waiting for an elected Parallel Hash participant to finish allocating more buckets"},
	{"IPC", "HashGrowBucketsReinsert", "Waiting for other Parallel Hash participants to finish inserting tuples into new buckets"},
	{"IPC", "LogicalApplySendData", "Waiting for a logical replication leader apply process to send data to a parallel apply process"},
	{"IPC", "LogicalParallelApplyStateChange", "Waiting for a logical replication parallel apply process to change state"},
	{"IPC", "LogicalSyncData", "Waiting for a logical replication remote server to send data for initial table synchronization"},
	{"IPC", "LogicalSyncStateChange", "Waiting for a logical replication remote server to change state"},
	{"IPC", "MessageQueueInternal", "Waiting for another process to be attached to a shared message queue"},
	{"IPC", "MessageQueuePutMessage", "Waiting to write a protocol message to a shared message queue"},
	{"IPC", "MessageQueueReceive", "Waiting to receive bytes from a shared message queue"},
	{"IPC", "MessageQueueSend", "Waiting to send bytes to a shared message queue"},
	{"IPC", "MultixactCreation", "Waiting for a multixact creation to complete"},
	{"IPC", "ParallelBitmapScan", "Waiting for parallel bitmap scan to become initialized"},
	{"IPC", "ParallelCreateIndexScan", "Waiting for parallel CREATE INDEX workers to finish heap scan"},
	{"IPC", "ParallelFinish", "Waiting for parallel workers to finish computing"},
	{"IPC", "ProcarrayGroupUpdate", "Waiting for the group leader to clear the transaction ID at transaction end"},
	{"IPC", "ProcSignalBarrier", "Waiting for a barrier event to be processed by all backends"},
	{"IPC", "Promote", "Waiting for standby promotion"},
	{"IPC", "RecoveryConflictSnapshot", "Waiting for recovery conflict resolution for a vacuum cleanup"},
	{"IPC", "RecoveryConflictTablespace", "Waiting for recovery conflict resolution for dropping a tablespace"},
	{"IPC", "RecoveryEndCommand", "Waiting for recovery_end_command to complete"},
	{"IPC", "RecoveryPause", "Waiting for recovery to be resumed"},
	{"IPC", "ReplicationOriginDrop", "Waiting for a replication origin to become inactive so it can be dropped"},
	{"IPC", "ReplicationSlotDrop", "Waiting for a replication slot to become inactive so it can be dropped"},
	{"IPC", "RestoreCommand", "Waiting for restore_command to complete"},
	{"IPC", "SafeSnapshot", "Waiting to obtain a valid snapshot for a READ ONLY DEFERRABLE transaction"},
	{"IPC", "SyncRep", "Waiting for confirmation from a remote server during synchronous replication"},
	{"IPC", "WalReceiverExit", "Waiting for the WAL receiver to exit"},
	{"IPC", "WalReceiverWaitStart", "Waiting for startup process to send initial data for streaming replication"},
	{"IPC", "WalSummaryReady", "Waiting for a new WAL summary to be generated"},
	{"IPC", "XactGroupUpdate", "Waiting for the group leader to update transaction status at transaction end"},
	{"Lock", "advisory", "Waiting to acquire an advisory user lock"},
	{"Lock", "applytransaction", "Waiting to acquire a lock on a remote transaction being applied by a logical replication subscriber"},
	{"Lock", "extend", "Waiting to extend a relation"},
	{"Lock", "frozenid", "Waiting to update pg_database.datfrozenxid and pg_database.datminmxid"},
	{"Lock", "object", "Waiting to acquire a lock on a non-relation database object"},
	{"Lock", "page", "Waiting to acquire a lock on a page of a relation"},
	{"Lock", "relation", "Waiting to acquire a lock on a relation"},
	{"Lock", "spectoken", "Waiting to acquire a speculative insertion lock"},
	{"Lock", "transactionid", "Waiting for a transaction to finish"},
	{"Lock", "tuple", "Waiting to acquire a lock on a tuple"},
	{"Lock", "userlock", "Waiting to acquire a user lock"},
	{"Lock", "virtualxid", "Waiting to acquire a virtual transaction ID lock"},
	{"LWLock", "AddinShmemInit", "Waiting to manage an extension\'s space allocation in shared memory"},
	{"LWLock", "AutoFile", "Waiting to update the postgresql.auto.conf file"},
	{"LWLock", "Autovacuum", "Waiting to read or update the current state of autovacuum workers"},
	{"LWLock", "AutovacuumSchedule", "Waiting to ensure that a table selected for autovacuum still needs vacuuming"},
	{"LWLock", "BackgroundWorker", "Waiting to read or update background worker state"},
	{"LWLock", "BtreeVacuum", "Waiting to read or update vacuum-related information for a B-tree index"},
	{"LWLock", "BufferContent", "Waiting to access a data page in memory"},
	{"LWLock", "BufferMapping", "Waiting to associate a data block with a buffer in the buffer pool"},
	{"LWLock", "CheckpointerComm", "Waiting to manage fsync requests"},
	{"LWLock", "CommitTs", "Waiting to read or update the last value set for a transaction commit timestamp"},
	{"LWLock", "CommitTsBuffer", "Waiting for I/O on a commit timestamp SLRU buffer"},
	{"LWLock", "CommitTsSLRU", "Waiting to access the commit timestamp SLRU cache"},
	{"LWLock", "ControlFile", "Waiting to read or update the pg_control file or create a new WAL file"},
	{"LWLock", "DSMRegistry", "Waiting to read or update the dynamic shared memory registry"},
	{"LWLock", "DSMRegistryDSA", "Waiting to access dynamic shared memory registry\'s dynamic shared memory allocator"},
	{"LWLock", "DSMRegistryHash", "Waiting to access dynamic shared memory registry\'s shared hash table"},
	{"LWLock", "DynamicSharedMemoryControl", "Waiting to read or update dynamic shared memory allocation information"},
	{"LWLock", "InjectionPoint", "Waiting to read or update information related to injection points"},
	{"LWLock", "LockFastPath", "Waiting to read or update a process\' fast-path lock information"},
	{"LWLock", "LockManager", "Waiting to read or update information about \"heavyweight\" locks"},
	{"LWLock", "LogicalRepLauncherDSA", "Waiting to access logical replication launcher\'s dynamic shared memory allocator"},
	{"LWLock", "LogicalRepLauncherHash", "Waiting to access logical replication launcher\'s shared hash table"},
	{"LWLock", "LogicalRepWorker", "Waiting to read or update the state of logical replication workers"},
	{"LWLock", "MultiXactGen", "Waiting to read or update shared multixact state"},
	{"LWLock", "MultiXactMemberBuffer", "Waiting for I/O on a multixact member SLRU buffer"},
	{"LWLock", "MultiXactMemberSLRU", "Waiting to access the multixact member SLRU cache"},
	{"LWLock", "MultiXactOffsetBuffer", "Waiting for I/O on a multixact offset SLRU buffer"},
	{"LWLock", "MultiXactOffsetSLRU", "Waiting to access the multixact offset SLRU cache"},
	{"LWLock", "MultiXactTruncation", "Waiting to read or truncate multixact information"},
	{"LWLock", "NotifyBuffer", "Waiting for I/O on a NOTIFY message SLRU buffer"},
	{"LWLock", "NotifyQueue", "Waiting to read or update NOTIFY messages"},
	{"LWLock", "NotifyQueueTail", "Waiting to update limit on NOTIFY message storage"},
	{"LWLock", "NotifySLRU", "Waiting to access the NOTIFY message SLRU cache"},
	{"LWLock", "OidGen", "Waiting to allocate a new OID"},
	{"LWLock", "ParallelAppend", "Waiting to choose the next subplan during Parallel Append plan execution"},
	{"LWLock", "ParallelHashJoin", "Waiting to synchronize workers during Parallel Hash Join plan execution"},
	{"LWLock", "ParallelQueryDSA", "Waiting for parallel query dynamic shared memory allocation"},
	{"LWLock", "ParallelVacuumDSA", "Waiting for parallel vacuum dynamic shared memory allocation"},
	{"LWLock", "PerSessionDSA", "Waiting for parallel query dynamic shared memory allocation"},
	{"LWLock", "PerSessionRecordType", "Waiting to access a parallel query\'s information about composite types"},
	{"LWLock", "PerSessionRecordTypmod", "Waiting to access a parallel query\'s information about type modifiers that identify anonymous record types"},
	{"LWLock", "PerXactPredicateList", "Waiting to access the list of predicate locks held by the current serializable transaction during a parallel query"},
	{"LWLock", "PgStatsData", "Waiting for shared memory stats data access"},
	{"LWLock", "PgStatsDSA", "Waiting for stats dynamic shared memory allocator access"},
	{"LWLock", "PgStatsHash", "Waiting for stats shared memory hash table access"},
	{"LWLock", "PredicateLockManager", "Waiting to access predicate lock information used by serializable transactions"},
	{"LWLock", "ProcArray", "Waiting to access the shared per-process data structures (typically, to get a snapshot or report a session\'s transaction ID)"},
	{"LWLock", "RelationMapping", "Waiting to read or update a pg_filenode.map file (used to track the filenode assignments of certain system catalogs)"},
	{"LWLock", "RelCacheInit", "Waiting to read or update a pg_internal.init relation cache initialization file"},
	{"LWLock", "ReplicationOrigin", "Waiting to create, drop or use a replication origin"},
	{"LWLock", "ReplicationOriginState", "Waiting to read or update the progress of one replication origin"},
	{"LWLock", "ReplicationSlotAllocation", "Waiting to allocate or free a replication slot"},
	{"LWLock", "ReplicationSlotControl", "Waiting to read or update replication slot state"},
	{"LWLock", "ReplicationSlotIO", "Waiting for I/O on a replication slot"},
	{"LWLock", "SerialBuffer", "Waiting for I/O on a serializable transaction conflict SLRU buffer"},
	{"LWLock", "SerialControl", "Waiting to read or update shared pg_serial state"},
	{"LWLock", "SerializableFinishedList", "Waiting to access the list of finished serializable transactions"},
	{"LWLock", "SerializablePredicateList", "Waiting to access the list of predicate locks held by serializable transactions"},
	{"LWLock", "SerializableXactHash", "Waiting to read or update information about serializable transactions"},
	{"LWLock", "SerialSLRU", "Waiting to access the serializable transaction conflict SLRU cache"},
	{"LWLock", "SharedTidBitmap", "Waiting to access a shared TID bitmap during a parallel bitmap index scan"},
	{"LWLock", "SharedTupleStore", "Waiting to access a shared tuple store during parallel query"},
	{"LWLock", "ShmemIndex", "Waiting to find or allocate space in shared memory"},
	{"LWLock", "SInvalRead", "Waiting to retrieve messages from the shared catalog invalidation queue"},
	{"LWLock", "SInvalWrite", "Waiting to add a message to the shared catalog invalidation queue"},
	{"LWLock", "SubtransBuffer", "Waiting for I/O on a sub-transaction SLRU buffer"},
	{"LWLock", "SubtransSLRU", "Waiting to access the sub-transaction SLRU cache"},
	{"LWLock", "SyncRep", "Waiting to read or update information about the state of synchronous replication"},
	{"LWLock", "SyncScan", "Waiting to select the starting location of a synchronized table scan"},
	{"LWLock", "TablespaceCreate", "Waiting to create or drop a tablespace"},
	{"LWLock", "TwoPhaseState", "Waiting to read or update the state of prepared transactions"},
	{"LWLock", "WaitEventCustom", "Waiting to read or update custom wait events information"},
	{"LWLock", "WALBufMapping", "Waiting to replace a page in WAL buffers"},
	{"LWLock", "WALInsert", "Waiting to insert WAL data into a memory buffer"},
	{"LWLock", "WALSummarizer", "Waiting to read or update WAL summarization state"},
	{"LWLock", "WALWrite", "Waiting for WAL buffers to be written to disk"},
	{"LWLock", "WrapLimitsVacuum", "Waiting to update limits on transaction id and multixact consumption"},
	{"LWLock", "XactBuffer", "Waiting for I/O on a transaction status SLRU buffer"},
	{"LWLock", "XactSLRU", "Waiting to access the transaction status SLRU cache"},
	{"LWLock", "XactTruncation", "Waiting to execute pg_xact_status or update the oldest transaction ID available to it"},
	{"LWLock", "XidGen", "Waiting to allocate a new transaction ID"},
	{"Timeout", "BaseBackupThrottle", "Waiting during base backup when throttling activity"},
	{"Timeout", "CheckpointWriteDelay", "Waiting between writes while performing a checkpoint"},
	{"Timeout", "PgSleep", "Waiting due to a call to pg_sleep or a sibling function"},
	{"Timeout", "RecoveryApplyDelay", "Waiting to apply WAL during recovery because of a delay setting"},
	{"Timeout", "RecoveryRetrieveRetryInterval", "Waiting during recovery when WAL data is not available from any source (pg_wal, archive or stream)"},
	{"Timeout", "RegisterSyncRequest", "Waiting while sending synchronization requests to the checkpointer, because the request queue is full"},
	{"Timeout", "SpinDelay", "Waiting while acquiring a contended spinlock"},
	{"Timeout", "VacuumDelay", "Waiting in a cost-based vacuum delay point"},
	{"Timeout", "VacuumTruncate", "Waiting to acquire an exclusive lock to truncate off any empty pages at the end of a table vacuumed"},
	{"Timeout", "WalSummarizerError", "Waiting after a WAL summarizer error"},
	/* end of list */
	{NULL, NULL, NULL}
};


/*
 * pg_get_wait_events
 *
 * List information about wait events (type, name and description).
 */
Datum
pg_get_wait_events(PG_FUNCTION_ARGS)
{
#define PG_GET_WAIT_EVENTS_COLS 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char	  **waiteventnames;
	int			nbwaitevents;

	/* Build tuplestore to hold the result rows */
	InitMaterializedSRF(fcinfo, 0);

	/* Iterate over the list of wait events */
	for (int idx = 0; waitEventData[idx].type != NULL; idx++)
	{
		Datum		values[PG_GET_WAIT_EVENTS_COLS] = {0};
		bool		nulls[PG_GET_WAIT_EVENTS_COLS] = {0};

		values[0] = CStringGetTextDatum(waitEventData[idx].type);
		values[1] = CStringGetTextDatum(waitEventData[idx].name);
		values[2] = CStringGetTextDatum(waitEventData[idx].description);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	/* Handle custom wait events for extensions */
	waiteventnames = GetWaitEventCustomNames(PG_WAIT_EXTENSION,
											 &nbwaitevents);

	for (int idx = 0; idx < nbwaitevents; idx++)
	{
		StringInfoData buf;
		Datum		values[PG_GET_WAIT_EVENTS_COLS] = {0};
		bool		nulls[PG_GET_WAIT_EVENTS_COLS] = {0};


		values[0] = CStringGetTextDatum("Extension");
		values[1] = CStringGetTextDatum(waiteventnames[idx]);

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "Waiting for custom wait event \"%s\" defined by extension module",
						 waiteventnames[idx]);

		values[2] = CStringGetTextDatum(buf.data);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	/* Likewise for injection points */
	waiteventnames = GetWaitEventCustomNames(PG_WAIT_INJECTIONPOINT,
											 &nbwaitevents);

	for (int idx = 0; idx < nbwaitevents; idx++)
	{
		StringInfoData buf;
		Datum		values[PG_GET_WAIT_EVENTS_COLS] = {0};
		bool		nulls[PG_GET_WAIT_EVENTS_COLS] = {0};


		values[0] = CStringGetTextDatum("InjectionPoint");
		values[1] = CStringGetTextDatum(waiteventnames[idx]);

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "Waiting for injection point \"%s\"",
						 waiteventnames[idx]);

		values[2] = CStringGetTextDatum(buf.data);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}
