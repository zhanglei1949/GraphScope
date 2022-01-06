package com.alibaba.graphscope.parallel.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLException;
import org.apache.giraph.comm.requests.WritableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * handling Response.
 */
public class NettyClientHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
    private AtomicInteger messageReceivedCount;

    public NettyClientHandler(){
        messageReceivedCount = new AtomicInteger(0);
    }

    public AtomicInteger getMessageReceivedCount(){
        return messageReceivedCount;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg){
        if (!(msg instanceof ByteBuf)) {
            throw new IllegalStateException("channelRead: Got a " +
                "non-ByteBuf message " + msg);
        }
        ByteBuf buf = (ByteBuf) msg;
        if (buf.readableBytes() < 4){
            throw new IllegalStateException("Expect at least 4 bytes response");
        }
        int seq = buf.readInt();
        int cnt = messageReceivedCount.addAndGet(1);
        logger.info("Client handler receive: " + seq + " from server, current msg count: " + cnt);
        logger.info("notify waiting on response cnt");
        synchronized (messageReceivedCount){
            messageReceivedCount.notifyAll();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("channelClosed: Closed the channel on " +
                ctx.channel().remoteAddress());
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        logger.warn("exceptionCaught: Channel channelId=" +
                ctx.channel().hashCode() + " failed with remote address " +
                ctx.channel().remoteAddress(), cause);
    }
}

//public class NettyClientHandler extends SimpleChannelInboundHandler<WritableRequest> {
//    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
//
//    /**
//     * <strong>Please keep in mind that this method will be renamed to
//     * {@code messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
//     * <p>
//     * Is called for each message of type {@link }.
//     *
//     * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
//     *            belongs to
//     * @param msg the message to handle
//     * @throws Exception is thrown if an error occurred
//     */
//    @Override
//    protected void channelRead0(ChannelHandlerContext ctx, WritableRequest msg) throws Exception {
//        logger.info("Client doesn't expect message from server");
//    }
//
//    @Override
//    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        super.channelInactive(ctx);
//        logger.info("channelClosed: Closed the channel on " + ctx.channel().remoteAddress());
//        ctx.fireChannelInactive();
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//        // Close the connection when an exception is raised.
//        cause.printStackTrace();
//        ctx.close();
//    }
//}
