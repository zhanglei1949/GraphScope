package com.alibaba.graphscope.parallel.netty;

import static org.apache.giraph.conf.GiraphConstants.CLIENT_RECEIVE_BUFFER_SIZE;
import static org.apache.giraph.conf.GiraphConstants.CLIENT_SEND_BUFFER_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_CONN_TRY_ATTEMPTS;

import com.alibaba.graphscope.parallel.netty.handler.NettyClientHandler;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import com.alibaba.graphscope.parallel.netty.request.serialization.WritableRequestEncoder;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import com.google.common.collect.MapMaker;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jnr.ffi.annotations.In;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyClient {

    private static Logger logger = LoggerFactory
        .getLogger(NettyClient.class);

    /**
     * 30 seconds to connect by default
     */
    public static final int MAX_CONNECTION_MILLISECONDS_DEFAULT = 30 * 1000;
    /**
     * Send buffer size
     */
    private final int sendBufferSize;
    /**
     * Receive buffer size
     */
    private final int receiveBufferSize;
    /**
     * Warn if request size is bigger than the buffer size by this factor
     */
    private final float requestSizeWarningThreshold;
    /**
     * Maximum thread pool size
     */
    private final int maxPoolSize;

    private ImmutableClassesGiraphConfiguration conf;
    /**
     * Key: worker id, value: ip:port
     */
    private final Map<Integer, InetSocketAddress> workerId2Address =
        new MapMaker().makeMap();
    /**
     * Giraph using waiting connections to characterize this list. We don't do so since it is
     * unnecessary.
     */
    private Connection[] connections;
    /**
     * All connected channels. length = workerNum, [index] = null
     */
    private Channel[] channels;
    private NettyClientHandler[] handlers;
//    private Map<Integer, LinkedList<ChannelFuture>> pendingRequests;
    /**
     * Map between dst frag id -> number of requests sent.
     */
    private Map<Integer,Integer> pendingRequests;
    private NetworkMap networkMap;

    private EventLoopGroup workGroup;

    private Bootstrap bootstrap;

    private int workerId;

    public NettyClient(
        ImmutableClassesGiraphConfiguration conf,
        NetworkMap networkMap,
        final UncaughtExceptionHandler exceptionHandler) {
        this.conf = conf;
        this.networkMap = networkMap;
        workerId = networkMap.getSelfWorkerId();
        /** Init constants */
        /** Number of threads for client to use, i.e. number of handlers*/
        maxPoolSize = GiraphConstants.NETTY_CLIENT_THREADS.get(conf);
        sendBufferSize = CLIENT_SEND_BUFFER_SIZE.get(conf);
        receiveBufferSize = CLIENT_RECEIVE_BUFFER_SIZE.get(conf);
        requestSizeWarningThreshold =
            GiraphConstants.REQUEST_SIZE_WARNING_THRESHOLD.get(conf);
        /**
         * Don't connect to self. But as we send message using index, we still make it length=workerNum,
         * but channels[workerId] = null and connections[workerId] = null
         */
        connections = new Connection[networkMap.getWorkerNum()];
        channels = new Channel[networkMap.getWorkerNum()];
        handlers = new NettyClientHandler[networkMap.getWorkerNum()];
        pendingRequests = new HashMap<>();
        for (int i = 0; i < networkMap.getWorkerNum(); ++i) {
//            pendingRequests.put(i, new LinkedList<ChannelFuture>());
        }
        /**
         * Start the client. connect to all address.
         */
        startClient(exceptionHandler);

    }


    void startClient(final UncaughtExceptionHandler exceptionHandler) {
        workGroup =
            new NioEventLoopGroup(
                1,
                ThreadUtils.createThreadFactory(
                    "netty-client-worker-" + networkMap.getSelfWorkerId() + "-%d",
                    exceptionHandler));

        bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                MAX_CONNECTION_MILLISECONDS_DEFAULT)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_SNDBUF, sendBufferSize)
            .option(ChannelOption.SO_RCVBUF, receiveBufferSize)
            //TODO: debug direct or not.
            .option(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
            .group(workGroup)
            .channel(NioSocketChannel.class)
            .handler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new WritableRequestEncoder(conf));
//                        p.addLast(new WritableRequestDecoder(conf));
//                        p.addLast()
                        p.addLast(
                            new NettyClientHandler(workerId));
                    }

                    @Override
                    public void channelUnregistered(ChannelHandlerContext ctx) throws
                        Exception {
                        super.channelUnregistered(ctx);
                        logger.error("Channel failed " + ctx.channel());
//                        checkRequestsAfterChannelFailure(ctx.channel());
                    }
                });
    }

    /**
     * Put connectToAllAddress out size of Constructor.
     */
    public void connectToAllAddress() {
        for (int dstWorkerId = 0; dstWorkerId < networkMap.getWorkerNum(); ++dstWorkerId) {
            if (dstWorkerId == networkMap.getSelfWorkerId()) {
                //TODO: better way also good performace?
                connections[dstWorkerId] = null;
                continue;
            }
            String hostName = networkMap.getHostNameForWorker(dstWorkerId);
            int port = networkMap.getPortForWorker(dstWorkerId);
            InetSocketAddress dstAddress = resolveAddress(hostName, port);
            //There are no duplicated connections in our settings.
            workerId2Address.put(dstWorkerId, dstAddress);
            logger.debug("Resolved address for worker: " + dstWorkerId + ": " + dstAddress);

            ChannelFuture channelFuture = bootstrap.connect(dstAddress);
            connections[dstWorkerId] = new Connection(channelFuture, dstAddress, dstWorkerId);
        }
        waitAllConnections();
        info("All connection established!");
    }

    /**
     * Wait for all connections established.
     */
    private void waitAllConnections() {
        int maxTries = MAX_CONN_TRY_ATTEMPTS.get(conf);
        int index = 0;
        int successCnt = 1;
        while (successCnt < connections.length) {
            if (Objects.nonNull(connections[index])) {
                Connection connection = connections[index];
                info("try for connection: " + connection.dstWorkerId + " while success connection: "
                    + successCnt);

                int failedCnt = 0;

                Channel channel = null;
                while (failedCnt < maxTries) {
                    //If this future fails, we need to update the future.
                    ChannelFuture future = connection.future;
                    boolean res = false;
                    try {
                        res = future.await(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new IllegalStateException("Future waiting exception");
                    }
                    if (res && future.isSuccess() && future.channel().isOpen()) {
                        logger.info(
                            "connection " + connection + " success for " + failedCnt + " times");
                        channel = future.channel();
                        break;
                    } else {
                        warn("Failed connection [" + workerId + " -> " + index + "] , tries: "
                            + failedCnt + "/"
                            + maxTries + ", success: " + future.isSuccess() + ", opened: " + future
                            .channel()
                            .isOpen() + ", cause: " + future.cause());
                        try {
                            TimeUnit.SECONDS.sleep(1);
                            failedCnt += 1;
                            //When encounter failure, we update the futreu;
                            ChannelFuture newFuture = bootstrap.connect(connection.address);
                            connection.updateFuture(newFuture);
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(
                                "Interrupted when waiting sleep: " + connection.address);
                        }
                    }

                }
                if (Objects.isNull(channel)) {
                    warn("Skip connection:" + connection + "for next time. try others");
                } else {
                    successCnt += 1;
                    channels[index] = channel;
                    handlers[index] = (NettyClientHandler) channel.pipeline().last();
                }
            } else {
                info("Connection to self is not needed [" + index + "]");
            }
            index = (index + 1) % connections.length;
        }
        checkChannels();
    }

    private void checkChannels() {
        for (int i = 0; i < channels.length; ++i) {
            if (i != workerId && Objects.isNull(channels[i])) {
                throw new IllegalStateException(
                    "Found invalid channel: " + channels[i] + " open:" + channels[i].isOpen());
            }
        }
    }

    /**
     * Verify whether an address is reachable.
     *
     * @param hostNameOrIp hostname or ip
     * @param port         dst port
     * @return resolved address
     */
    private InetSocketAddress resolveAddress(String hostNameOrIp, int port) {
        int resolveAttempts = 0;
        InetSocketAddress address = new InetSocketAddress(hostNameOrIp, port);
        while (address.isUnresolved() &&
            resolveAttempts < conf.getInetAddressMaxResolveTime()) {
            ++resolveAttempts;
            logger.warn("resolveAddress: Failed to resolve " + address +
                " on attempt " + resolveAttempts + " of " +
                conf.getInetAddressMaxResolveTime() + " attempts, sleeping for 1 seconds");
            ThreadUtils.trySleep(1000);
            address = new InetSocketAddress(hostNameOrIp,
                address.getPort());
        }
        if (resolveAttempts >= conf.getInetAddressMaxResolveTime()) {
            throw new IllegalStateException("resolveAddress: Couldn't " +
                "resolve " + address + " in " + resolveAttempts + " tries.");
        }
        return address;
    }

    public void sendMessage(int dstFragId, WritableRequest request) {
        if (dstFragId == workerId) {
            throw new IllegalStateException("Shouldn't reach here:" + dstFragId + ", " + workerId);
        }
        ChannelFuture requestFuture = channels[dstFragId].writeAndFlush(request);
//        pendingRequests.get(dstFragId).offer(requestFuture);
        //Must already been initialized to 0 in presuperstep.
        pendingRequests.put(dstFragId, pendingRequests.get(dstFragId) + 1);
        debug("send msg " + request + " to [" + dstFragId + "], corresponding pending request: "
//            + pendingRequests.get(dstFragId).size());
            + pendingRequests.get(dstFragId));
    }

    /**
     * Hide flush from message manager. From message manager view, all request send immediately.
     */
    private void flushMessages() {
        debug("flushing messages");
        for (int i = 0; i < networkMap.getWorkerNum(); ++i) {
            if (i == workerId) {
                continue;
            }
            channels[i].flush();
        }
        debug("finish flushing messages");
    }

    public void preSuperStep(){
        for (int i = 0; i < networkMap.getWorkerNum(); ++i) {
//            pendingRequests.get(i).clear();
            pendingRequests.put(i, 0);
            if (i != workerId){
                handlers[i].preSuperStep();
            }
        }
    }

    public void waitAllRequests() {
        flushMessages();
        for (int i = 0; i < networkMap.getWorkerNum(); ++i) {
//            LinkedList<ChannelFuture> futures = pendingRequests.get(i);
//            for (ChannelFuture future : futures) {
//                if (future.isDone()) {
//                    if (future.isSuccess()) {
//                        debug("message to [" + i + "] success");
//                    } else {
//                        error("message to [" + i + "] failed: " + future.cause());
//                    }
//                } else {
//                    try {
//                        warn("message to [" + i + "] not done, waiting...");
//                        future.await();
//                        warn("Ok,message to [" + i + "]  done, wake up");
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
        }
        info("Message sending success, waiting for response");
        for (int i = 0; i < networkMap.getWorkerNum(); ++i) {
            if (i == workerId) {
                continue;
            }
            NettyClientHandler handler = handlers[i];
//            handler.waitForResponse(pendingRequests.get(i).size());
            handler.waitForResponse(pendingRequests.get(i));
            info("response waiting finished");
        }
        info("finish waiting sending all messages");
    }

    @Override
    public String toString() {
        String res = "NettyClient: [" + workerId + "] channels:";
        for (Channel channel : channels) {
            if (Objects.nonNull(channel)) {
                res += channel.remoteAddress() + ",";
            }
        }
        return res;
    }

    public void close() {
        waitAllRequests();
        try {
            for (int i = 0; i < channels.length; ++i) {
                if (Objects.nonNull(channels[i])) {
                    channels[i].close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        workGroup.shutdownGracefully();
        info("Closing...");
    }

    private void warn(String msg) {
        logger.warn(
            "NettyClient: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread
                .currentThread()
                .getId() + "]: " + msg);
    }

    private void debug(String msg) {
        logger.debug(
            "NettyClient: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread
                .currentThread()
                .getId() + "]: " + msg);
    }

    private void info(String msg) {
        logger.info(
            "NettyClient: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread
                .currentThread()
                .getId() + "]: " + msg);
    }

    private void error(String msg) {
        logger.error(
            "NettyClient: [" + networkMap.getSelfWorkerId() + "], Thread: [" + Thread
                .currentThread()
                .getId() + "]: " + msg);
    }

    /**
     * The abstraction of connection.
     */
    private static class Connection {

        /**
         * Future object
         */
        private ChannelFuture future;
        /**
         * Address of the future
         */
        private final InetSocketAddress address;
        /**
         * grape worker id(MPI_COMM)
         */
        private final Integer dstWorkerId;

        /**
         * Constructor.
         *
         * @param future   Immutable future
         * @param address  Immutable address
         * @param workerId Immutable taskId
         */
        Connection(
            ChannelFuture future, InetSocketAddress address, Integer workerId) {
            this.future = future;
            this.address = address;
            this.dstWorkerId = workerId;
        }

        /**
         * We can update the future but don't update address.
         *
         * @param future future obj.
         */
        public void updateFuture(ChannelFuture future) {
            this.future = future;
        }

        @Override
        public String toString() {
            return "(future=" + future + ",address=" + address + ",dstWorkerId=" +
                dstWorkerId + ")";
        }
    }
}
