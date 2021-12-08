/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.worker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WorkerContext allows for the execution of user code on a per-worker basis. There's one
 * WorkerContext per worker.
 */
@SuppressWarnings("rawtypes")
public interface WorkerContext
    extends WorkerAggregator,Writable, WorkerIndexUsage<WritableComparable> {
    /**
     * Initialize the WorkerContext. This method is executed once on each Worker before the first
     * superstep starts.
     *
     * @throws IllegalAccessException Thrown for getting the class
     * @throws InstantiationException Expected instantiation in this method.
     */
    void preApplication() throws InstantiationException,IllegalAccessException;

    /**
     * Finalize the WorkerContext. This method is executed once on each Worker after the last superstep
     * ends.
     */
    void postApplication();

    /**
     * Execute user code. This method is executed once on each Worker before each superstep starts.
     */
    void preSuperstep();

    /**
     * Get messages which other workers sent to this worker and clear them (can be called once per
     * superstep)
     *
     * @return Messages received
     */
    List<Writable> getAndClearMessagesFromOtherWorkers();

    /**
     * Send message to another worker
     *
     * @param message     Message to send
     * @param workerIndex Index of the worker to send the message to
     */
    void sendMessageToWorker(Writable message, int workerIndex);

    /**
     * Execute user code. This method is executed once on each Worker after each superstep ends.
     */
    void postSuperstep();

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    long getSuperstep();

    /**
     * Get the total (all workers) number of vertices that existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    long getTotalNumVertices();

    /**
     * Get the total (all workers) number of edges that existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    long getTotalNumEdges();

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    Mapper.Context getContext();

    /**
     * Call this to log a line to command line of the job. Use in moderation - it's a synchronous
     * call to Job client
     *
     * @param line Line to print
     */
    void logToCommandLine(String line);
}
