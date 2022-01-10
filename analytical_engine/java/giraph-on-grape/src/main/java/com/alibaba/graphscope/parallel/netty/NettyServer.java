package com.alibaba.graphscope.parallel.netty;

import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;

import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.handler.NettyServerHandler;
import com.alibaba.graphscope.parallel.netty.request.serialization.WritableRequestDecoder;
import com.alibaba.graphscope.parallel.netty.request.serialization.WritableRequestEncoder;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer<OID_T extends WritableComparable,GS_VID_T> {

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /**
     * Send buffer size
     */
    private final int sendBufferSize;
    /**
     * Receive buffer size
     */
    private final int receiveBufferSize;
    /**
     * TCP backlog
     */
    private final int tcpBacklog;

    /**
     * Boss eventloop group
     */
    private final EventLoopGroup bossGroup;
    /**
     * Worker eventloop group
     */
    private final EventLoopGroup workerGroup;

    private int bossThreadSize;
    private int workerThreadSize;
    private NetworkMap networkMap;
    private InetSocketAddress myAddress;
    private ImmutableClassesGiraphConfiguration conf;
    private MessageStore<OID_T, Writable,GS_VID_T> nextIncomingMessages;
    private SimpleFragment fragment;
    private List<NettyServerHandler<OID_T,GS_VID_T>> handlers;
    private Channel channel;
    /**
     * Accepted channels
     */
    private final ChannelGroup accepted = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    ServerBootstrap bootstrap;

    public NettyServer(
        ImmutableClassesGiraphConfiguration conf,
        SimpleFragment fragment,
        NetworkMap networkMap,
        MessageStore<OID_T, Writable,GS_VID_T> nextIncomingMessages,
        final UncaughtExceptionHandler exceptionHandler) {
        this.conf = conf;
        this.networkMap = networkMap;
        this.nextIncomingMessages = nextIncomingMessages;
        this.fragment = fragment;
        handlers = new ArrayList<>();

        bossThreadSize = GiraphConstants.NETTY_SERVER_BOSS_THREADS.get(conf);
        workerThreadSize = GiraphConstants.NETTY_SERVER_WORKER_THREADS.get(conf);
        sendBufferSize = GiraphConstants.SERVER_SEND_BUFFER_SIZE.get(conf);
        receiveBufferSize = GiraphConstants.SERVER_RECEIVE_BUFFER_SIZE.get(conf);

        // SO_BACKLOG controls  number of clients our server can simultaneously listen.
        tcpBacklog = conf.getWorkerNum();

        bossGroup =
            new NioEventLoopGroup(
                bossThreadSize,
                ThreadUtils.createThreadFactory("netty-server-boss-" +  networkMap.getSelfWorkerId() +"-%d", exceptionHandler));
        workerGroup =
            new NioEventLoopGroup(
                workerThreadSize,
                ThreadUtils.createThreadFactory(
                    "netty-server-worker-" +  networkMap.getSelfWorkerId() +"-%d", exceptionHandler));
    }

    public void startServer() {
        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, tcpBacklog)
            .option(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
            .handler(new LoggingHandler(LogLevel.INFO))
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_SNDBUF, sendBufferSize)
            .childOption(ChannelOption.SO_RCVBUF, receiveBufferSize)
            .childOption(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
            .childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(receiveBufferSize / 4,
                    receiveBufferSize, receiveBufferSize))

            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(
                            new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx)
                                    throws Exception {
                                    accepted.add(ctx.channel());
                                    ctx.fireChannelActive();
                                }
                            });
                        //TODO: optimization with fixed-frame
//                        p.addLast(new WritableRequestEncoder(conf));
                        p.addLast(new WritableRequestDecoder(conf));
                        p.addLast("handler", getHandler());
                    }
                });
        bindAddress();
    }

    private synchronized NettyServerHandler<OID_T,GS_VID_T> getHandler(){
        NettyServerHandler<OID_T,GS_VID_T> handler = new NettyServerHandler<OID_T,GS_VID_T>(fragment, nextIncomingMessages);
        handlers.add(handler);
        logger.info("creating handler: " + handler + " current size: " + handlers.size());
        return handler;
    }

    private void bindAddress(){
        int myPort = networkMap.getSelfPort();
        String myHostNameOrIp = networkMap.getSelfHostNameOrIp();
        int maxAttempts = MAX_IPC_PORT_BIND_ATTEMPTS.get(conf);
        int curAttempt = 0;
        while (curAttempt < maxAttempts){
            info("try binding  port: " + myPort + " for " + curAttempt + "/" + maxAttempts + " times");
            try {
                this.myAddress = new InetSocketAddress(myHostNameOrIp, myPort);
                ChannelFuture f = bootstrap.bind(myAddress).sync();

                accepted.add(f.channel());
//                channel = f.channel();
                break;
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            } catch (Exception e) {
                // CHECKSTYLE: resume IllegalCatchCheck
                warn("start: Likely failed to bind on attempt " +
                    curAttempt + " to port " + myPort + e.getCause().toString());
                ++curAttempt;
            }
        }
        info("start: Started server " +
            "communication server: " + myAddress + " with up to " +
            workerThreadSize + " threads on bind attempt " + curAttempt +
            " with sendBufferSize = " + sendBufferSize +
            " receiveBufferSize = " + receiveBufferSize);
    }

    public void preSuperStep(MessageStore<OID_T, Writable,GS_VID_T> nextIncomingMessages){
        logger.info("Pre super step for handlers of size: " + handlers.size() + ": " + handlers);
        for (NettyServerHandler handler : handlers){
            handler.preSuperStep(nextIncomingMessages);
        }
    }

    private void warn(String msg) {
        logger.warn(
            "NettyServer: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread.currentThread()
                .getId() + "]: " + msg);
    }

    private void debug(String msg) {
        logger.debug(
            "NettyServer: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread.currentThread()
                .getId() + "]: " + msg);
    }

    private void info(String msg) {
        logger.info(
            "NettyServer: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread.currentThread()
                .getId() + "]: " + msg);
    }

    public void close() {
        try {
            debug("Closing channels of size: " + accepted.size());
            accepted.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        info("channels down...");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        info("thread groups down..");

        info("Successfully close server " + myAddress);
    }
}
