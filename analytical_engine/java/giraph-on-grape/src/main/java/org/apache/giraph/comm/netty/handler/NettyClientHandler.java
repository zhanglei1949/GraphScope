package org.apache.giraph.comm.netty.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.HashMap;
import java.util.Map;
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
public class NettyClientHandler extends SimpleChannelInboundHandler<Object> {

    //    private ByteBuf content;
    private ChannelHandlerContext ctx;
    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
    private Map<String, Writable> result;
    private AggregatorManager aggregatorManager;

    public NettyClientHandler(AggregatorManager aggregatorManager) {
        this.aggregatorManager = aggregatorManager;
        result = new HashMap<>();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
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
                DataInput stream = new DataInputStream(new ByteArrayInputStream(aggregatorMessage.getData()));
                Writable value = ReflectionUtils.newInstance(
                    aggregatorManager.getAggregatedValue(aggregatorMessage.getAggregatorId())
                        .getClass());
                value.readFields(stream);
                logger.info(
                    "client: aggregator message: " + aggregatorMessage.getMessageType().name()
                        + ",value: " + aggregatorMessage.getValue() + ", " + value.toString());
            } else {
                logger.error("not a aggregator message");
            }
        } else {
            logger.error("Expect a byte buffer");
        }
    }

    public Writable getAggregatedMessage(String aggregatorId) {
        if (!result.containsKey(aggregatorId)) {
            logger.error("result not available");
            return null;
        }
        return result.get(aggregatorId);
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
