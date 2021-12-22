package org.apache.giraph.comm.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.netty.handler.NettyClientHandler;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyMessageDecoder;
import org.apache.giraph.comm.requests.NettyMessageEncoder;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation class which do responsible for all netty-related stuff.
 */
public class NettyClient {

    private static Logger logger = LoggerFactory.getLogger(NettyClient.class);
    /**
     * 30 seconds to connect by default
     */
    public static final int MAX_CONNECTION_MILLISECONDS_DEFAULT = 30 * 1000;

    public static final int SIZE = 256;

    private WorkerInfo workerInfo;
    private EventLoopGroup workGroup;
    //    private ChannelFuture channelFuture;
    private Channel channel;
    private ImmutableClassesGiraphConfiguration conf;
    private AggregatorManager aggregatorManager;
    private NettyClientHandler handler;

    public NettyClient(
        ImmutableClassesGiraphConfiguration conf,
        AggregatorManager aggregatorManager,
        WorkerInfo workerInfo,
        final Thread.UncaughtExceptionHandler exceptionHandler) {
        this.workerInfo = workerInfo;
        this.conf = conf;
        this.aggregatorManager = aggregatorManager;
        workGroup =
            new NioEventLoopGroup(
                1,
                ThreadUtils.createThreadFactory(
                    "netty-client-worker-%d", exceptionHandler));

        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.group(workGroup)
            .channel(NioSocketChannel.class)
            .handler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new NettyMessageEncoder());
                        p.addLast(new NettyMessageDecoder());
                        p.addLast(new NettyClientHandler(aggregatorManager));
                    }
                });
        // Make the connection attempt.
        ChannelFuture future = b.connect(workerInfo.getHost(), workerInfo.getInitPort());

        int failureTime = 0;
        while (failureTime < 10){
            if (!future.isSuccess() || !future.channel().isOpen()){
                logger.info("failed for " + failureTime + " times");
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                failureTime += 1;
            }
            else {
                logger.info("success for " + failureTime + " times");
                channel = future.channel();;
            }
        }
        if (failureTime >= 10){
            logger.error("connection failed");
            return ;
        }

        handler = (NettyClientHandler) channel.pipeline().last();
    }

    public void sendMessage(NettyMessage request) {
        ChannelFuture channelFuture = channel.writeAndFlush(request);
        try {
            channelFuture.await();
            logger.info("send msg: " + request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Writable getAggregatedMessage(String aggregatorId) {
        return handler.getAggregatedMessage(aggregatorId);
    }

    public void close() {
        try {
            if (!Objects.isNull(channel)) {
                channel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        workGroup.shutdownGracefully();
        //        try {
        //            channelFuture.channel().closeFuture().sync();
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }
        logger.info("shut down client");
    }
}
