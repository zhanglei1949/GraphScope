package com.alibaba.graphscope.parallel.netty.request.serialization;

import com.alibaba.graphscope.parallel.mm.GiraphMessageManagerFactory;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyMessageType;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RequestUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableRequestDecoder  extends ByteToMessageDecoder {
    private static Logger logger = LoggerFactory.getLogger(WritableRequestDecoder.class);

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the
     * input {@link ByteBuf} has nothing to read when return from this method or till nothing was
     * read from the input {@link ByteBuf}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs
     *            to
     * @param in  the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        throws Exception {
        if (in.readableBytes() < 4 + 1) {
            return;
        }
        in.markReaderIndex();
        //num of bytes
        int numBytes = in.readInt();
        if (numBytes < 0){
            logger.error("Expect a positive number of bytes");
            ctx.close();
            return ;
        }
        // Decode the request type
        int enumValue = in.readByte();
        RequestType type = RequestType.values()[enumValue];
        Class<? extends WritableRequest> messageClass = type.getClazz();

        if (in.readableBytes() < numBytes){
            in.resetReaderIndex();
            logger.error("not enough num bytes: " + in.readableBytes() + " expected: " + numBytes);
            return ;
        }

        logger.debug(
                "decode: Client "
                    + messageClass.getName()
                    + ", with size "
                    + in.readableBytes());

        WritableRequest request = ReflectionUtils.newInstance(messageClass);
        request = RequestUtils.decodeWritableRequest(in, request);
        logger.debug("decode res: " + request);
        out.add(request);
    }
}
