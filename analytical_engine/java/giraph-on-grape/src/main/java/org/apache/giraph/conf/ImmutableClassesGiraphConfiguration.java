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

import com.alibaba.graphscope.fragment.SimpleFragment;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * The classes set here are immutable, the remaining configuration is mutable. Classes are immutable
 * and final to provide the best performance for instantiation.  Everything is thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("unchecked")
public class ImmutableClassesGiraphConfiguration<I extends WritableComparable,
    V extends Writable, E extends Writable> extends GiraphConfiguration{
    /** Holder for all the classes */
    private final GiraphClasses classes;
    /** holder for grape fragment class types */
    private final GrapeTypes grapeClasses;

    private static String DEFAULT_WORKER_FILE_PREFIX= "giraph-on-grape";


    public ImmutableClassesGiraphConfiguration(Configuration configuration, SimpleFragment fragment){
        super(configuration);
        classes = new GiraphClasses<I,V,E>(configuration);
        grapeClasses = new GrapeTypes(fragment);
    }


    /**
     * Create a vertex
     *
     * @return Instantiated vertex
     */
    public Vertex<I, V, E> createVertex() {
        Class vertexClass = classes.getVertexClass();
        try {
            return (Vertex<I, V, E>) vertexClass.newInstance();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean hasVertexOutputFormat() {
        return classes.hasVertexOutputFormat();
    }

    /**
     * Get the user's subclassed
     * {@link org.apache.giraph.io.VertexOutputFormat}.
     *
     * @return User's vertex output format class
     */
    public Class<? extends VertexOutputFormat<I, V, E>>
    getVertexOutputFormatClass() {
        return classes.getVertexOutputFormatClass();
    }

    /**
     * Create a user vertex output format class.
     * Note: Giraph should only use WrappedVertexOutputFormat,
     * which makes sure that Configuration parameters are set properly.
     *
     * @return Instantiated user vertex output format class
     */
    private VertexOutputFormat<I, V, E> createVertexOutputFormat() {
        Class<? extends VertexOutputFormat<I, V, E>> klass =
            getVertexOutputFormatClass();
        return ReflectionUtils.newInstance(klass, this);
    }

    /**
     * Generate the string for default work file to write data to. shall be unique for each run.
     */
    public String getDefaultWorkerFile(){
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        return String.join("-", DEFAULT_WORKER_FILE_PREFIX, getComputationClass().getSimpleName(), timeStamp);
    }

    /**
     * Get the user's subclassed WorkerContext.
     *
     * @return User's worker context class
     */
    @Override
    public Class<? extends WorkerContext> getWorkerContextClass() {
        return classes.getWorkerContextClass();
    }

    @Override
    public Class<? extends Computation<I, V, E,
            ? extends Writable, ? extends Writable>>
    getComputationClass() {
        return classes.getComputationClass();
    }

    /**
     * Get the user's subclassed vertex index class.
     *
     * @return User's vertex index class
     */
    public Class<I> getVertexIdClass() {
        return classes.getVertexIdClass();
    }

    public  I createVertexId(){
        return (I) ReflectionUtils.newInstance(classes.getVertexIdClass());
    }

    public Class<V> getVertexValueClass(){
        return classes.getVertexValueClass();
    }

    public V createVertexValue(){
        return (V) ReflectionUtils.newInstance(classes.getVertexValueClass());
    }

    public Class<E> getEdgeValueClass(){
        return classes.getEdgeValueClass();
    }

    public E createEdgeValue(){
        return (E)ReflectionUtils.newInstance(classes.getEdgeValueClass());
    }

    public Writable createInComingMessageValue(){
        return (Writable) ReflectionUtils.newInstance(classes.getIncomingMessageClass());
    }

    public Writable createOutgoingMessageValue(){
        return (Writable) ReflectionUtils.newInstance(classes.getOutgoingMessageClass());
    }

    public Class<?> getGrapeOidClass(){
        return grapeClasses.getOidClass();
    }

    public Class<?> getGrapeVidClass(){
        return grapeClasses.getVidClass();
    }

    public Class<?> getGrapeVdataClass(){
        return grapeClasses.getVdataClass();
    }

    public Class<?> getGrapeEdataClass(){
        return grapeClasses.getEdataClass();
    }
}
