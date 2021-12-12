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

import static org.apache.giraph.conf.Constants.VERTEX_CLASS;

import java.util.HashMap;
import java.util.Map;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Holder for classes used by Giraph.
 *
 * @param <I> Vertex ID class
 * @param <V> Vertex Value class
 * @param <E> Edge class
 */
@SuppressWarnings("unchecked")
public class GiraphClasses<I extends WritableComparable,
    V extends Writable, E extends Writable> {

    /** Generic types used to describe graph */
    protected GiraphTypes<I, V, E> giraphTypes;

    /** Vertex input format class - cached for fast access */
    protected Class<? extends VertexInputFormat<I, V, E>>
        vertexInputFormatClass;
    /** Vertex output format class - cached for fast access */
    protected Class<? extends VertexOutputFormat<I, V, E>>
        vertexOutputFormatClass;

    /** Computation class - cached for fast access */
    protected Class<? extends Computation<I, V, E,
            ? extends Writable, ? extends Writable>>
        computationClass;
    /** Worker context class - cached for fast access */
    protected Class<? extends WorkerContext> workerContextClass;

//    /** Edge input format class - cached for fast access */
//    protected Class<? extends EdgeInputFormat<I, E>>
//        edgeInputFormatClass;
//    /** Edge output format class - cached for fast access */
//    protected Class<? extends EdgeOutputFormat<I, V, E>>
//        edgeOutputFormatClass;

    public GiraphClasses() {
        giraphTypes = new GiraphTypes<I, V, E>();
    }

    /**
     * Get Vertex implementation class
     *
     * @return Vertex implementation class
     */
    public Class<? extends Vertex> getVertexClass() {
        return giraphTypes.getVertexClass();
    }

    /**
     * Check if VertexOutputFormat is set
     *
     * @return true if VertexOutputFormat is set
     */
    public boolean hasVertexOutputFormat() {
        return vertexOutputFormatClass != null;
    }

    /**
     * Get VertexOutputFormat set
     *
     * @return VertexOutputFormat
     */
    public Class<? extends VertexOutputFormat<I, V, E>>
    getVertexOutputFormatClass() {
        return vertexOutputFormatClass;
    }

    /**
     * Set WorkerContext used
     *
     * @param workerContextClass WorkerContext class to set
     * @return this
     */
    public GiraphClasses setWorkerContextClass(
        Class<? extends WorkerContext> workerContextClass) {
        this.workerContextClass = workerContextClass;
        return this;
    }

    /**
     * Check if WorkerContext is set
     *
     * @return true if WorkerContext is set
     */
    public boolean hasWorkerContextClass() {
        return workerContextClass != null;
    }

    /**
     * Get WorkerContext used
     *
     * @return WorkerContext
     */
    public Class<? extends WorkerContext> getWorkerContextClass() {
        return workerContextClass;
    }

    /**
     * Get Computation class
     *
     * @return Computation class.
     */
    public Class<? extends Computation<I, V, E,
        ? extends Writable, ? extends Writable>>
    getComputationClass() {
        return computationClass;
    }

}
