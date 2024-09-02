/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.Vertex;

public interface Nbr<VID_T, EDATA_T> {
    String type();

    /**
     * Get the neighboring vertex.
     *
     * @return vertex.
     */
    Vertex<VID_T> neighbor();

    /**
     * Get the edge data.
     *
     * @return edge data.
     */
    EDATA_T data();

    Nbr<VID_T, EDATA_T> inc();

    boolean eq(Nbr<VID_T, EDATA_T> rhs);

    Nbr<VID_T, EDATA_T> dec();

    void delete();

    long getAddress();
}
