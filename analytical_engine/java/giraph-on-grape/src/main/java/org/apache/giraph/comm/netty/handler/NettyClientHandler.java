package org.apache.giraph.comm.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.requests.AggregatorMessage;
import org.apache.giraph.comm.requests.NettyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a client-side channel.
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<Object> {

//    private ByteBuf content;
    private ChannelHandlerContext ctx;
    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;

        // Initialize the message.
        //content = ctx.alloc().directBuffer(NettyClient.SIZE).writeZero(NettyClient.SIZE);

        // Send the initial messages.
        //generateTraffic();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("channelClosed: Closed the channel on " +
            ctx.channel().remoteAddress());

        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Server is supposed to send nothing, but if it sends something, discard it.
        logger.info("Client receive msg from server");
        if (msg instanceof NettyMessage) {
            NettyMessage message = (NettyMessage) msg;
            if (message instanceof AggregatorMessage) {
                AggregatorMessage aggregatorMessage = (AggregatorMessage) message;
                logger.info("client: aggregator message: " + aggregatorMessage.getMessageType().name());
            }
            else {
                logger.error("not a aggregator message");
            }
        } else {
            logger.error("Expect a byte buffer");
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
