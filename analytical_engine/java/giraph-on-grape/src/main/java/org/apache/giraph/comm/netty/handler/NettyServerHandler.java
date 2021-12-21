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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a server-side channel.
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<Object> {
    private static Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        // discard
        if (msg instanceof Writable){
            Writable writable = (Writable) msg;
            if (writable instanceof LongWritable){
                LongWritable longWritable = (LongWritable) writable;
                logger.info("Received msg: " + longWritable.get());
            }
            else if (writable instanceof DoubleWritable){
                DoubleWritable doubleWritable = (DoubleWritable) writable;
                logger.info("Received msg: " + doubleWritable.get());
            }
            else {
                logger.error("Not allowed type");
            }
        }
        else {
            logger.error("Expect a writable instance.");
        }
        // Send the response with the request id
        ByteBuf buffer = ctx.alloc().buffer(4);
        buffer.writeInt(1);
        ctx.write(buffer);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
