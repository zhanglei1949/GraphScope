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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RequestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes encoded requests from the client.
 */
public class RequestDecoder extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(RequestDecoder.class);
    /**
     * Time class to use
     */
    private static final Time TIME = SystemTime.get();
    /**
     * Configuration
     */
    private final ImmutableClassesGiraphConfiguration conf;
//    /** In bound byte counter to output */
//    private final InboundByteCounter byteCounter;
    /**
     * Start nanoseconds for the decoding time
     */
    private long startDecodingNanoseconds = -1;

    /**
     * Constructor.
     *
     * @param conf Configuration
     */
    public RequestDecoder(ImmutableClassesGiraphConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
        if (!(msg instanceof ByteBuf)) {
            throw new IllegalStateException("decode: Got illegal message " + msg);
        }

        if (logger.isDebugEnabled()) {
            startDecodingNanoseconds = TIME.getNanoseconds();
        }

        // Decode the request
        ByteBuf buf = (ByteBuf) msg;
        int numberOfBytes =buf.readInt();
        logger.info("Number of bytes: " + numberOfBytes);
        int enumValue = buf.readByte();
        RequestType type = RequestType.values()[enumValue];
        Class<? extends WritableRequest> requestClass = type.getRequestClass();
        WritableRequest request =
            ReflectionUtils.newInstance(requestClass, conf);
        request = RequestUtils.decodeWritableRequest(buf, request);

        if (logger.isDebugEnabled()) {
            logger.debug("decode: Client " + request.getClientId() +
                ", requestId " + request.getRequestId() +
                ", " + request.getType() + ", with size " +
                buf.writerIndex() + " took " +
                Times.getNanosSince(TIME, startDecodingNanoseconds) + " ns");
        }
        ReferenceCountUtil.release(buf);
        // fire writableRequest object to upstream handlers
        ctx.fireChannelRead(request);
    }
}
