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

package org.apache.giraph.conf;

import io.netty.buffer.ByteBufAllocator;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;

/**
 * Adds user methods specific to Giraph. This will be put into an ImmutableClassesGiraphConfiguration
 * that provides the configuration plus the immutable classes.
 *
 * <p>Keeps track of parameters which were set so it easily set them in another copy of
 * configuration.
 */
public class GiraphConfiguration extends Configuration implements GiraphConstants {

    /**
     * Constructor that creates the configuration
     */
    public GiraphConfiguration() {
    }

    /**
     * Constructor.
     *
     * @param conf Configuration
     */
    public GiraphConfiguration(Configuration conf) {
        super(conf);
    }


    /**
     * Set the worker context class (optional)
     *
     * @param workerContextClass Determines what code is executed on a each worker before and after
     *     each superstep and computation
     */
    public final void setWorkerContextClass(Class<? extends WorkerContext> workerContextClass) {
        WORKER_CONTEXT_CLASS.set(this, workerContextClass);
    }

    public  Class<? extends WorkerContext> getWorkerContextClass(){
        return WORKER_CONTEXT_CLASS.get(this);
    }

    /**
     * Set the computation class(user app).
     *
     * @param appClass User specified computation class.
     */
    public final void setComputationClass(Class<? extends AbstractComputation> appClass) {
        COMPUTATION_CLASS.set(this, appClass);
    }

    /**
     * Get the user's subclassed {@link Computation}
     *
     * @return User's computation class
     */
    public Class<? extends Computation> getComputationClass() {
        return COMPUTATION_CLASS.get(this);
    }

    /**
     * Set vertex input class.
     *
     * @param vertexInputFormatClass User specified computation class.
     */
    public final void setVertexInputFormatClass(Class<? extends VertexInputFormat> vertexInputFormatClass){
        VERTEX_INPUT_FORMAT_CLASS.set(this, vertexInputFormatClass);
    }

    /**
     * Set vertex input class.
     *
     * @param vertexInputFormatClass User specified computation class.
     */
    public final void getVertexInputFormatClass(Class<? extends VertexInputFormat> vertexInputFormatClass){
        VERTEX_INPUT_FORMAT_CLASS.get(this);
    }

    /**
     * Does the job have a {@link org.apache.giraph.io.VertexOutputFormat}?
     *
     * @return True iff a {@link org.apache.giraph.io.VertexOutputFormat} has been specified.
     */
    public boolean hasVertexOutputFormat() {
        return VERTEX_OUTPUT_FORMAT_CLASS.get(this) != null;
    }

    /**
     * Set the message combiner class (optional)
     *
     * @param messageCombinerClass Determines how vertex messages are combined
     */
    public void setMessageCombinerClass(Class<? extends MessageCombiner> messageCombinerClass) {
        MESSAGE_COMBINER_CLASS.set(this, messageCombinerClass);
    }

    /**
     * Set the master class (optional)
     *
     * @param masterComputeClass Runs master computation
     */
    public final void setMasterComputeClass(Class<? extends MasterCompute> masterComputeClass) {
        MASTER_COMPUTE_CLASS.set(this, masterComputeClass);
    }

}
