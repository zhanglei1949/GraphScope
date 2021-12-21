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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.requests.MasterRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

/**
 * Handler for requests on master
 */
public class MasterRequestServerHandler extends
    RequestServerHandler<MasterRequest> {

    /**
     * Constructor
     *
     * @param conf             Configuration
     * @param myTaskInfo       Current task info
     * @param exceptionHandler Handles uncaught exceptions
     */
    public MasterRequestServerHandler(
        ImmutableClassesGiraphConfiguration conf,
        WorkerInfo myTaskInfo,
        Thread.UncaughtExceptionHandler exceptionHandler) {
        super(conf, myTaskInfo, exceptionHandler);
    }

    @Override
    public void processRequest(MasterRequest request) {
//        request.doRequest(commHandler);
    }

    /**
     * Factory for {@link MasterRequestServerHandler}
     */
    public static class Factory implements RequestServerHandler.Factory {

        /**
         * Constructor
         */
        public Factory() {
        }

        @Override
        public RequestServerHandler newHandler(
            ImmutableClassesGiraphConfiguration conf,
            WorkerInfo myTaskInfo,
            Thread.UncaughtExceptionHandler exceptionHandler) {
            return new MasterRequestServerHandler(conf,
                myTaskInfo, exceptionHandler);
        }
    }
}
