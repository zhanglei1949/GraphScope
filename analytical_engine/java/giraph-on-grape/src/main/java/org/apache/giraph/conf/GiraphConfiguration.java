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
     * ByteBufAllocator to be used by netty
     */
    private ByteBufAllocator nettyBufferAllocator = null;

    /**
     * Constructor that creates the configuration
     */
    public GiraphConfiguration() {
//        configureHadoopSecurity();
    }

    /**
     * Constructor.
     *
     * @param conf Configuration
     */
    public GiraphConfiguration(Configuration conf) {
        super(conf);
//        configureHadoopSecurity();
    }



}
