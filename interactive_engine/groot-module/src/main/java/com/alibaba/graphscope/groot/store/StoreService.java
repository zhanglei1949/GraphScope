/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.AvgMetric;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.store.external.ExternalStorage;
import com.alibaba.graphscope.groot.store.jna.JnaGraphStore;
import com.alibaba.graphscope.proto.groot.GraphDefPb;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class StoreService implements MetricsAgent {
    private static final Logger logger = LoggerFactory.getLogger(StoreService.class);

    private static final String PARTITION_WRITE_PER_SECOND_MS = "partition.write.per.second.ms";

    private final Configs storeConfigs;
    private final int storeId;
    private final int writeThreadCount;
    private final int compactThreadCount;
    private final MetaService metaService;
    private Map<Integer, GraphPartition> idToPartition;
    private ExecutorService writeExecutor;
    private ExecutorService ingestExecutor;
    private ExecutorService garbageCollectExecutor;
    private ExecutorService compactExecutor;

    private ThreadPoolExecutor downloadExecutor;
    private final boolean enableGc;
    private volatile boolean shouldStop = true;
    private final boolean isSecondary;

    private volatile long lastUpdateTime;
    private Map<Integer, AvgMetric> partitionToMetric;

    public StoreService(
            Configs storeConfigs, MetaService metaService, MetricsCollector metricsCollector) {
        this.storeConfigs = storeConfigs;
        this.storeId = CommonConfig.NODE_IDX.get(storeConfigs);
        this.enableGc = StoreConfig.STORE_GC_ENABLE.get(storeConfigs);
        this.writeThreadCount = StoreConfig.STORE_WRITE_THREAD_COUNT.get(storeConfigs);
        this.compactThreadCount = StoreConfig.STORE_COMPACT_THREAD_NUM.get(storeConfigs);
        this.metaService = metaService;
        this.isSecondary = CommonConfig.SECONDARY_INSTANCE_ENABLED.get(storeConfigs);
        metricsCollector.register(this, this::updateMetrics);
    }

    public void start() throws IOException {
        logger.info("starting StoreService...");
        List<Integer> partitionIds = this.metaService.getPartitionsByStoreId(this.storeId);
        this.idToPartition = new HashMap<>(partitionIds.size());
        for (int partitionId : partitionIds) {
            try {
                GraphPartition partition = makeGraphPartition(this.storeConfigs, partitionId);
                this.idToPartition.put(partitionId, partition);
            } catch (IOException e) {
                throw new GrootException(e);
            }
        }
        initMetrics();
        this.shouldStop = false;
        this.writeExecutor =
                new ThreadPoolExecutor(
                        writeThreadCount,
                        writeThreadCount,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-write", logger));
        this.ingestExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-ingest", logger));
        int partitionCount = partitionIds.size();
        int compactQueueLength =
                partitionCount - this.compactThreadCount <= 0
                        ? 1
                        : partitionCount - this.compactThreadCount;
        this.compactExecutor =
                new ThreadPoolExecutor(
                        1,
                        this.compactThreadCount,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(compactQueueLength),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-compact", logger));
        this.garbageCollectExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-garbage-collect", logger));
        logger.info("StoreService started. storeId [" + this.storeId + "]");
        this.downloadExecutor =
                new ThreadPoolExecutor(
                        16,
                        16,
                        1000L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "store-download", logger));
        this.downloadExecutor.allowCoreThreadTimeOut(true);
        logger.info("StoreService started. storeId [" + this.storeId + "]");
    }

    public GraphPartition makeGraphPartition(Configs configs, int partitionId) throws IOException {
        return new JnaGraphStore(configs, partitionId);
    }

    public Map<Integer, GraphPartition> getIdToPartition() {
        return this.idToPartition;
    }

    public int getStoreId() {
        return storeId;
    }

    public void stop() {
        this.shouldStop = true;
        if (this.idToPartition != null) {
            CountDownLatch latch = new CountDownLatch(this.idToPartition.size());
            for (GraphPartition partition : this.idToPartition.values()) {
                this.writeExecutor.execute(
                        () -> {
                            try {
                                partition.close();
                                logger.info("partition #[" + partition.getId() + "] closed");
                            } catch (IOException e) {
                                logger.error(
                                        "partition #[" + partition.getId() + "] close failed", e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }
            try {
                long waitSeconds = 30L;
                if (!latch.await(waitSeconds, TimeUnit.SECONDS)) {
                    logger.warn("not all partitions closed, waited [" + waitSeconds + "] seconds");
                }
            } catch (InterruptedException e) {
                // Ignore
            }
            this.idToPartition = null;
        }
        if (this.writeExecutor != null) {
            this.writeExecutor.shutdown();
            try {
                this.writeExecutor.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.writeExecutor = null;
        }
    }

    public boolean batchWrite(StoreDataBatch storeDataBatch)
            throws ExecutionException, InterruptedException {
        long snapshotId = storeDataBatch.getSnapshotId();
        List<Map<Integer, OperationBatch>> dataBatch = storeDataBatch.getDataBatch();
        AtomicBoolean hasDdl = new AtomicBoolean(false);
        int maxRetry = 10;
        for (Map<Integer, OperationBatch> partitionToBatch : dataBatch) {
            while (!shouldStop && partitionToBatch.size() != 0 && maxRetry > 0) {
                partitionToBatch = writeStore(snapshotId, partitionToBatch, hasDdl);
                maxRetry--;
            }
        }
        return hasDdl.get();
    }

    private Map<Integer, OperationBatch> writeStore(
            long snapshotId, Map<Integer, OperationBatch> partitionToBatch, AtomicBoolean hasDdl)
            throws ExecutionException, InterruptedException {
        Map<Integer, OperationBatch> batchNeedRetry = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(partitionToBatch.size());
        CompletableFuture<Object> future = new CompletableFuture<>();
        for (Map.Entry<Integer, OperationBatch> e : partitionToBatch.entrySet()) {
            int partitionId = e.getKey();
            OperationBatch batch = e.getValue();
            logger.debug("writeStore partition [" + partitionId + "]");
            this.writeExecutor.execute(
                    () -> {
                        try {
                            if (partitionId != -1) {
                                // Ignore Marker
                                // Only support partition operation for now
                                long beforeWriteTime = System.nanoTime();
                                GraphPartition partition = this.idToPartition.get(partitionId);
                                if (partition == null) {
                                    throw new IllegalStateException(
                                            "partition ["
                                                    + partitionId
                                                    + "] is not initialized / exists");
                                }
                                if (partition.writeBatch(snapshotId, batch)) {
                                    hasDdl.set(true);
                                }
                                long afterWriteTime = System.nanoTime();
                                this.partitionToMetric
                                        .get(partitionId)
                                        .add(afterWriteTime - beforeWriteTime);
                            }
                        } catch (Exception ex) {
                            logger.error(
                                    "write to partition [{}] failed, snapshotId [{}].",
                                    partitionId,
                                    snapshotId,
                                    ex);
                            String msg = "Not supported operation in secondary mode";
                            if (ex.getMessage().contains(msg)) {
                                logger.warn("Ignored write in secondary instance, {}", msg);
                            } else {
                                batchNeedRetry.put(partitionId, batch);
                            }
                        }
                        if (counter.decrementAndGet() == 0) {
                            future.complete(null);
                        }
                    });
        }
        future.get();
        if (batchNeedRetry.size() > 0) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        return batchNeedRetry;
    }

    public GraphDefPb getGraphDefBlob() throws IOException {
        GraphPartition graphPartition = this.idToPartition.get(0);
        return graphPartition.getGraphDefBlob();
    }

    public MetaService getMetaService() {
        return this.metaService;
    }

    public void ingestData(
            String path, Map<String, String> config, CompletionCallback<Void> callback) {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(storeConfigs);
        String downloadPath = StoreConfig.STORE_DATA_DOWNLOAD_PATH.get(storeConfigs);
        if (downloadPath.isEmpty()) {
            downloadPath = Paths.get(dataRoot, "download").toString();
        }
        String[] items = path.split("/");
        // Get the  unique path  (uuid)
        String unique_path = items[items.length - 1];
        Path uniquePath = Paths.get(downloadPath, unique_path);
        if (!Files.isDirectory(uniquePath)) {
            try {
                Files.createDirectories(uniquePath);
                logger.info("Created uniquePath {}", uniquePath);
            } catch (IOException e) {
                logger.error("create uniquePath failed. uniquePath {}", uniquePath, e);
                callback.onError(e);
                return;
            }
        }
        this.ingestExecutor.execute(
                () -> {
                    logger.info("ingesting data [{}]", path);
                    try {
                        ingestDataInternal(path, config, callback);
                    } catch (Exception e) {
                        logger.error("ingest data failed. path [" + path + "]", e);
                        callback.onError(e);
                    }
                    logger.info("ingest data [{}] complete", path);
                });
    }

    private void ingestDataInternal(
            String path, Map<String, String> config, CompletionCallback<Void> callback)
            throws IOException {
        ExternalStorage externalStorage = ExternalStorage.getStorage(path, config);
        Set<Map.Entry<Integer, GraphPartition>> entries = this.idToPartition.entrySet();
        AtomicInteger counter = new AtomicInteger(entries.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (Map.Entry<Integer, GraphPartition> entry : entries) {
            int pid = entry.getKey();
            GraphPartition partition = entry.getValue();
            String fileName = "part-r-" + String.format("%05d", pid) + ".sst";
            String fullPath = path + "/" + fileName;
            logger.info("downloading {}", fullPath);
            this.downloadExecutor.execute(
                    () -> {
                        try {
                            partition.ingestExternalFile(externalStorage, fullPath);
                        } catch (Exception e) {
                            logger.error("ingest external file failed.", e);
                            if (!finished.getAndSet(true)) {
                                callback.onError(e);
                            }
                        }
                        if (counter.decrementAndGet() == 0) {
                            logger.info("All download tasks finished.");
                            finished.set(true);
                            callback.onCompleted(null);
                        } else {
                            logger.info(counter.get() + " download tasks remaining");
                        }
                    });
        }
    }

    public void clearIngest(String uniquePath) throws IOException {
        if (uniquePath == null || uniquePath.isEmpty()) {
            logger.warn("Must set a sub-path for clearing.");
            return;
        }
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(storeConfigs);
        String downloadPath = StoreConfig.STORE_DATA_DOWNLOAD_PATH.get(storeConfigs);
        if (downloadPath.isEmpty()) {
            downloadPath = Paths.get(dataRoot, "download").toString();
        }
        downloadPath = Paths.get(downloadPath, uniquePath).toString();
        try {
            logger.info("Clearing directory {}", downloadPath);
            FileUtils.forceDelete(new File(downloadPath));
        } catch (FileNotFoundException e) {
            // Ignore
        }
        logger.info("cleared directory {}", downloadPath);
    }

    public void garbageCollect(long snapshotId, CompletionCallback<Void> callback) {
        if (!enableGc) {
            callback.onError(new GrootException("store gc is not enabled"));
            return;
        }
        this.garbageCollectExecutor.execute(
                () -> {
                    try {
                        // logger.debug("Garbage collecting, snapshot [{}]", snapshotId);
                        garbageCollectInternal(snapshotId);
                        callback.onCompleted(null);
                    } catch (Exception e) {
                        logger.error("garbage collect failed. snapshot [{}]", snapshotId, e);
                        callback.onError(e);
                    }
                });
    }

    private void garbageCollectInternal(long snapshotId) throws IOException {
        for (Map.Entry<Integer, GraphPartition> entry : this.idToPartition.entrySet()) {
            GraphPartition partition = entry.getValue();
            partition.garbageCollect(snapshotId);
        }
    }

    public void compactDB(CompletionCallback<Void> callback) {
        if (isSecondary) {
            callback.onCompleted(null);
            return;
        }
        int partitionCount = this.idToPartition.values().size();
        CountDownLatch compactCountDownLatch = new CountDownLatch(partitionCount);
        AtomicInteger successCompactJobCount = new AtomicInteger(partitionCount);
        logger.info("compact DB");
        for (GraphPartition partition : this.idToPartition.values()) {
            this.compactExecutor.execute(
                    () -> {
                        try {
                            partition.compact();
                            logger.info("Compaction {} partition finished", partition.getId());
                            successCompactJobCount.decrementAndGet();
                        } catch (Exception e) {
                            logger.error("compact DB failed", e);
                        } finally {
                            compactCountDownLatch.countDown();
                        }
                    });
        }

        try {
            compactCountDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("compact DB has been InterruptedException", e);
        }

        if (successCompactJobCount.get() > 0) {
            callback.onError(new Exception("not all partition compact success. please check log."));
        } else {
            callback.onCompleted(null);
        }
    }

    public void tryCatchUpWithPrimary() throws IOException {
        if (!isSecondary) {
            return;
        }
        for (GraphPartition partition : this.idToPartition.values()) {
            partition.tryCatchUpWithPrimary();
        }
    }

    public void reopenPartition(long wait_sec, CompletionCallback<Void> callback) {
        if (!isSecondary) {
            callback.onCompleted(null);
            return;
        }
        try {
            for (GraphPartition partition : this.idToPartition.values()) {
                partition.reopenSecondary(wait_sec);
            }
            callback.onCompleted(null);
        } catch (Exception e) {
            logger.error("reopen secondary failed", e);
            callback.onError(e);
        }
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();
        long interval = currentTime - this.lastUpdateTime;
        if (this.partitionToMetric != null) {
            this.partitionToMetric.values().forEach(m -> m.update(interval));
        }
        this.lastUpdateTime = currentTime;
    }

    @Override
    public void initMetrics() {
        this.lastUpdateTime = System.nanoTime();
        this.partitionToMetric = new HashMap<>();
        for (Integer id : this.idToPartition.keySet()) {
            this.partitionToMetric.put(id, new AvgMetric());
        }
    }

    @Override
    public Map<String, String> getMetrics() {
        List<String> partitionWritePerSecondMs =
                partitionToMetric.entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "%s:%s",
                                                entry.getKey(),
                                                (int) (1000 * entry.getValue().getAvg())))
                        .collect(Collectors.toList());
        return new HashMap<String, String>() {
            {
                put(PARTITION_WRITE_PER_SECOND_MS, String.valueOf(partitionWritePerSecondMs));
            }
        };
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {PARTITION_WRITE_PER_SECOND_MS};
    }

    public long[] getDiskStatus() {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(storeConfigs);
        File file = new File(dataRoot);
        long total = file.getTotalSpace();
        long usable = file.getUsableSpace();
        return new long[] {total, usable};
    }
}
