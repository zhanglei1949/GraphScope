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
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.giraph.conf.GiraphConstants.NETTY_SIMULATE_FIRST_REQUEST_CLOSED;

/**
 * Generic handler of requests.
 *
 * @param <R> Request type
 */
public abstract class RequestServerHandler<R> extends
    ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(NettyClient.class);

    /** Number of bytes in the encoded response */
    public static final int RESPONSE_BYTES = 16;
    /** Already closed first request? */
    private static volatile boolean ALREADY_CLOSED_FIRST_REQUEST = false;
    /** Close connection on first request (used for simulating failure) */
    private final boolean closeFirstRequest;
    /** My task info */
    private final WorkerInfo workerInfo;
    /** Start nanoseconds for the processing time */
    private long startProcessingNanoseconds = -1;
    /** Handler for uncaught exceptions */
    private final Thread.UncaughtExceptionHandler exceptionHandler;

    /**
     * Constructor
     *
     * @param conf Configuration
     * @param myTaskInfo Current task info
     * @param exceptionHandler Handles uncaught exceptions
     */
    public RequestServerHandler(
//        WorkerRequestReservedMap workerRequestReservedMap,
        ImmutableClassesGiraphConfiguration conf,
        WorkerInfo myTaskInfo,
        Thread.UncaughtExceptionHandler exceptionHandler) {
//        this.workerRequestReservedMap = workerRequestReservedMap;
        closeFirstRequest = NETTY_SIMULATE_FIRST_REQUEST_CLOSED.get(conf);
        this.workerInfo = myTaskInfo;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("messageReceived: Got " + msg.getClass());
        }

        WritableRequest request = (WritableRequest) msg;

        // Simulate a closed connection on the first request (if desired)
        if (closeFirstRequest && !ALREADY_CLOSED_FIRST_REQUEST) {
            logger.info("messageReceived: Simulating closing channel on first " +
                "request " + request.getRequestId() + " from " +
                request.getClientId());
            setAlreadyClosedFirstRequest();
            ctx.close();
            return;
        }

        processRequest((R) request);

        //no response
    }

    /**
     * Set the flag indicating already closed first request
     */
    private static void setAlreadyClosedFirstRequest() {
        ALREADY_CLOSED_FIRST_REQUEST = true;
    }

    /**
     * Process request
     *
     * @param request Request to process
     */
    public abstract void processRequest(R request);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("channelActive: Connected the channel on " +
                ctx.channel().remoteAddress());
        }
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("channelInactive: Closed the channel on " +
                ctx.channel().remoteAddress());
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(
        ChannelHandlerContext ctx, Throwable cause) throws Exception {
        exceptionHandler.uncaughtException(Thread.currentThread(), cause);
    }

    /**
     * Factory for {@link RequestServerHandler}
     */
    public interface Factory {
        /**
         * Create new {@link RequestServerHandler}
         *
         * @param conf Configuration to use
         * @param exceptionHandler Handles uncaught exceptions
         * @return New {@link RequestServerHandler}
         */
        RequestServerHandler newHandler(
            ImmutableClassesGiraphConfiguration conf,
            WorkerInfo workerInfo,
            Thread.UncaughtExceptionHandler exceptionHandler);

    }
}
