/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.giraph.comm.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.HashMap;
import java.util.Map;
import org.apache.giraph.comm.requests.AggregatorMessage;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a server-side channel.
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<Object> {

    private static Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);
    private AggregatorManager aggregatorManager;
    private Map<String, Integer> aggregateTimes;
    private ByteBufAllocator allocator;
    private ByteBuf buffer;

    public NettyServerHandler(AggregatorManager aggregatorManager) {
        this.aggregatorManager = aggregatorManager;
        this.aggregateTimes = new HashMap<>();
        this.allocator = new PooledByteBufAllocator();
        this.buffer = allocator.buffer();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NettyMessage) {
            NettyMessage message = (NettyMessage) msg;
            if (message instanceof AggregatorMessage) {
                AggregatorMessage aggregatorMessage = (AggregatorMessage) message;

                aggregatorManager.acceptAggregatorMessage(aggregatorMessage);
                String aggregatorId = aggregatorMessage.getAggregatorId();
                aggregateTimes.put(aggregatorId,
                    aggregateTimes.getOrDefault(aggregatorId, 0) + 1);
                logger.info(
                    "server: aggregator message: " + aggregatorMessage.getMessageType().name()
                        + "value: " + aggregatorMessage.getValue() + "result: " + aggregatorManager
                        .getAggregatedValue(aggregatorId));
                if (aggregateTimes.get(aggregatorId) == aggregatorManager.getNumWorkers()) {
                    //send msg to worker.
                    logger.info("server received " + aggregateTimes.get(aggregatorId)
                        + " times reduce, now broadcast");
                    Writable writable = aggregatorManager.getAggregatedValue(aggregatorId);
                    buffer.clear();
                    ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
                    writable.write(outputStream);
                    outputStream.flush();
                    AggregatorMessage toSend;
                    if (buffer.hasArray()) {
                        toSend = new AggregatorMessage(aggregatorId, aggregatorMessage.getValue(),
                            buffer.array());
                    } else {
                        byte[] bytes = new byte[buffer.readableBytes()];
                        buffer.getBytes(buffer.readerIndex(), bytes);
                        toSend = new AggregatorMessage(aggregatorId, aggregatorMessage.getValue(),
                            bytes);
                    }
                    ctx.writeAndFlush(toSend);
                    aggregateTimes.put(aggregatorId, 0);
                }
            } else {
                logger.error("Not a aggregator message");
            }
        } else {
            logger.error("Expect a netty message.");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
