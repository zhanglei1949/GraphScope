package com.alibaba.graphscope.parallel.netty.request.serialization;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.giraph.comm.requests.NettyMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableRequestEncoder extends MessageToByteEncoder {
    private static Logger logger = LoggerFactory.getLogger(WritableRequestEncoder.class);

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message
     * that can be handled by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs
     *            to
     * @param msg the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (msg instanceof WritableRequest){
            WritableRequest request = (WritableRequest) msg;
            int serializeBytes = request.getNumBytes() + SIZE_OF_BYTE + SIZE_OF_INT;
            out.capacity(serializeBytes);
            ByteBufOutputStream output = new ByteBufOutputStream(out);
            //write number of bytes for actual data.
            output.writeInt(request.getNumBytes());
            output.writeByte(request.getRequestType().ordinal());
            try {
                request.write(output);
            } catch (IndexOutOfBoundsException e) {
                logger.error("write: Most likely the size of request was not properly " +
                    "specified (this buffer is too small) - see getSerializedSize() " +
                    "in " + request.getRequestType().getClazz());
                throw new IllegalStateException(e);
            }
            output.flush();
            output.close();
            logger.info("Encode msg, type: " + request.getRequestType().getClazz().getName() + ", num bytes: " + request.getNumBytes() + ", ");
        }
        else {
            logger.error("Encoder: got instance " + msg + ", expect a WritableRequest");
        }
    }
}
