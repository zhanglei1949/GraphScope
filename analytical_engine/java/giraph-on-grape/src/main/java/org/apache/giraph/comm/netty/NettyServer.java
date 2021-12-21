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

package org.apache.giraph.comm.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.netty.handler.NettyServerHandler;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This server uses Netty and will implement all Giraph communication
 */
public class NettyServer {
    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);


    /**
     * Boss eventloop group
     */
    private final EventLoopGroup bossGroup;
    /**
     * Worker eventloop group
     */
    private final EventLoopGroup workerGroup;
    private int maxPoolSize;
    private ChannelFuture channelFuture;

    public NettyServer( WorkerInfo workerInfo,
        final Thread.UncaughtExceptionHandler exceptionHandler) {

//        maxPoolSize = GiraphConstants.NETTY_SERVER_THREADS.get(conf);
        maxPoolSize = 4;

        bossGroup = new NioEventLoopGroup(4,
            ThreadUtils.createThreadFactory(
                "netty-server-boss-%d", exceptionHandler));

        workerGroup = new NioEventLoopGroup(maxPoolSize,
            ThreadUtils.createThreadFactory(
                "netty-server-worker-%d", exceptionHandler));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new NettyServerHandler());
                    }
                });

            // Bind and start to accept incoming connections.
            channelFuture = b.bind(workerInfo.getInitPort()).sync();

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void close(){
        try {
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();

        logger.info("Successfully close server");
    }
}

