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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.giraph.comm.requests.AggregatorMessage;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyWritableMessage;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** Handles a server-side channel. */
public class NettyServerHandler extends SimpleChannelInboundHandler<Object> {

    private static Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);
    private AggregatorManagerNettyImpl aggregatorManager;
    // private Map<String, Integer> aggregateTimes;
    private ByteBufAllocator allocator;
    private ByteBuf buffer;
    private CountDownLatch aggregationLatch;
    private AtomicInteger msgNo;

    public NettyServerHandler(AggregatorManager aggregatorManager) {
        logger.info("Creating server hanlder in thread: " + Thread.currentThread().getName());
        this.aggregatorManager = (AggregatorManagerNettyImpl) aggregatorManager;
        this.allocator = new PooledByteBufAllocator();
        this.buffer = allocator.buffer();
        aggregationLatch = new CountDownLatch(1);
        msgNo = new AtomicInteger(0);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NettyMessage) {
            int no = msgNo.getAndAdd(1);
            logger.info("receiving: msg no." + no);
            NettyMessage message = (NettyMessage) msg;
            if (message instanceof AggregatorMessage) {
                AggregatorMessage aggregatorMessage = (AggregatorMessage) message;

                aggregatorManager.acceptNettyMessage(aggregatorMessage);
                String aggregatorId = aggregatorMessage.getAggregatorId();
                logger.info(
                        "server thread: ["
                                + Thread.currentThread().getId()
                                + "]: aggregating id: "
                                + aggregatorMessage.getAggregatorId()
                                + "value: "
                                + aggregatorMessage.getValue()
                                + "result: "
                                + aggregatorManager.getAggregatedValue(aggregatorId)
                                + " counts: "
                                + no
                                + ", need: "
                                + (aggregatorManager.getNumWorkers() - 1));

                NettyWritableMessage toSend = null;
                // if (no == aggregatorManager.getNumWorkers() - 1) {
                //     // send msg to worker.
                //     logger.info(
                //             "server "
                //                     + Thread.currentThread().getId()
                //                     + "received "
                //                     + no
                //                     + " times reduce, now broadcast");
                //     msgNo.set(0);
                //     msgNo.notifyAll();
                // } else {
                //     logger.info("server: " + Thread.currentThread().getId() + "begin waiting..");
                //     msgNo.wait();
                //     logger.info("server: " + Thread.currentThread().getId() + "finish waiting!");
                // }

                Writable writable = aggregatorManager.getAggregatedValue(aggregatorId);

                toSend = new NettyWritableMessage(writable, 100);
                logger.info("server to client: readable bytes: " + buffer.readableBytes());

                logger.info("server send response to client: " + toSend);
                ctx.writeAndFlush(toSend);

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
