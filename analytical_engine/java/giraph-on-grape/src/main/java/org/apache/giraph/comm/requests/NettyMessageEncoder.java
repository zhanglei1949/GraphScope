package org.apache.giraph.comm.requests;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encode a netty message obj into a bytebuffer.
 */
public class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(NettyMessageEncoder.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg,
        ChannelPromise promise) throws Exception {
        if (!(msg instanceof NettyMessage)) {
            throw new IllegalArgumentException(
                "encode: Got a message of type " + msg.getClass());
        }


        NettyMessage request = (NettyMessage) msg;
        int requestSize = request.getSerializedSize();
        if (requestSize <= 0){
            logger.error("Request size less than zero.");
            return ;
        }
        requestSize += SIZE_OF_BYTE;
        ByteBuf buf = ctx.alloc().buffer(requestSize);

        ByteBufOutputStream output = new ByteBufOutputStream(buf);

        // This will later be filled with the correct size of serialized request
        output.writeByte(request.getMessageType().ordinal());
        try {
            request.write(output);
        } catch (IndexOutOfBoundsException e) {
            logger.error("write: Most likely the size of request was not properly " +
                "specified (this buffer is too small) - see getSerializedSize() " +
                "in " + request.getMessageType().getRequestClass());
            throw new IllegalStateException(e);
        }
        output.flush();
        output.close();

        if (logger.isDebugEnabled()) {
            logger.debug("write: Client " +
                ", size = " + buf.readableBytes() + ", " +
                request.getMessageType() + " took ");
        }
        ctx.write(buf, promise);
    }

}
