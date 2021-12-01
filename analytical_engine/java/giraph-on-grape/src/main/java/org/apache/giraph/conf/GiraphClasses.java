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
import org.apache.giraph.graph.Vertex;
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

    private Map<String, Class<?>> giraphTypes;

    public GiraphClasses() {
        giraphTypes = new HashMap<>();
    }

    /**
     * Get Vertex implementation class
     *
     * @return Vertex implementation class
     */
    public Class<? extends Vertex> getVertexClass() {
        return (Class<? extends Vertex>) giraphTypes.get(VERTEX_CLASS);
    }

    public void setVertexClass(Class<? extends Vertex> vertexClass) {
        giraphTypes.put(VERTEX_CLASS, vertexClass);
    }

}
