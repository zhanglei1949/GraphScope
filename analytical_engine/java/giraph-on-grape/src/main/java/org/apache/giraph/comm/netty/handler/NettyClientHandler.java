package org.apache.giraph.comm.netty.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.requests.AggregatorMessage;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a client-side channel.
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    //    private ByteBuf content;
    private ChannelHandlerContext ctx;
    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);

    private AggregatorManager aggregatorManager;
    private BlockingQueue<Promise<NettyMessage>> messageList = new ArrayBlockingQueue<Promise<NettyMessage>>(10);

    public NettyClientHandler(AggregatorManager aggregatorManager) {
        this.aggregatorManager = aggregatorManager;

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        synchronized (this){
            Promise<NettyMessage> prom;
            while ((prom = messageList.poll()) != null){
                prom.setFailure(new IOException("Connection lost"));
            }
            messageList = null;
        }
        logger.info("channelClosed: Closed the channel on " +
            ctx.channel().remoteAddress());

        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        // Server is supposed to send nothing, but if it sends something, discard it.
        logger.info("Client receive msg from server");
        synchronized (this){
            if (messageList != null){
                messageList.poll().setSuccess(msg);
            }
            else {
                throw new IllegalStateException("message list null");
            }
        }
    }
    public Future<NettyMessage> sendMessage(NettyMessage request){
        if (ctx == null){
            throw new IllegalStateException("ctx empty");
        }
        return sendMessage(request, ctx.executor().newPromise());
    }

    public Future<NettyMessage> sendMessage(NettyMessage request, Promise<NettyMessage> promise){
        synchronized (this){
            if (messageList == null){
                //connection closed
                promise.setFailure(new IllegalStateException());
            }
            else if (messageList.offer(promise)){
                logger.info("client send msg:" + request + ", "+ promise.toString());
                ctx.writeAndFlush(request);
            }
            else {
                //message rejected.
                promise.setFailure(new BufferOverflowException());
            }
            return promise;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    private final ChannelFutureListener trafficGenerator = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
            if (future.isSuccess()) {
                //generateTraffic();
                logger.info("successfully send msg times: ");
            } else {
                future.cause().printStackTrace();
                future.channel().close();
            }
        }
    };
}
