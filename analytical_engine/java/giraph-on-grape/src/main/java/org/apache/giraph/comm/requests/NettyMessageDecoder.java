package org.apache.giraph.comm.requests;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RequestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyMessageDecoder extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(NettyMessageDecoder.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
        if (!(msg instanceof ByteBuf)) {
            throw new IllegalStateException("decode: Got illegal message " + msg);
        }

        // Decode the request
        ByteBuf buf = (ByteBuf) msg;
        int enumValue = buf.readByte();
        NettyMessageType type = NettyMessageType.values()[enumValue];
        Class<? extends NettyMessage> messageClass = type.getRequestClass();
        NettyMessage message =
            ReflectionUtils.newInstance(messageClass);
        message = RequestUtils.decodeNettyMessage(buf, message);

        if (logger.isDebugEnabled()) {
            logger.debug("decode: Client " + message.getMessageType() + ", with size " +
                buf.writerIndex() + " took ");
        }
        ReferenceCountUtil.release(buf);
        // fire writableRequest object to upstream handlers
        ctx.fireChannelRead(message);
    }

}
