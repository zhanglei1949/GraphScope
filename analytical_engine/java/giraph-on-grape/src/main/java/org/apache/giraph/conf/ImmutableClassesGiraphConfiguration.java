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

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;


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
    V extends Writable, E extends Writable> {
    /** Holder for all the classes */
    private final GiraphClasses classes;

    public ImmutableClassesGiraphConfiguration(){
        classes = new GiraphClasses<I,V,E>();
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
}
