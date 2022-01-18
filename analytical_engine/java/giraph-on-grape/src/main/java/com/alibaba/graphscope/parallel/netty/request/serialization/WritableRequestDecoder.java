package com.alibaba.graphscope.parallel.netty.request.serialization;

import com.alibaba.graphscope.parallel.mm.GiraphMessageManagerFactory;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyMessageType;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RequestUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableRequestDecoder  extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(WritableRequestDecoder.class);

    private ImmutableClassesGiraphConfiguration conf;
    private int waitingFullMsgTimes;
    private int decoderId;

    public WritableRequestDecoder(ImmutableClassesGiraphConfiguration conf, int decoderId){
        this.conf = conf;
        waitingFullMsgTimes = 0;
        this.decoderId = decoderId;
    }

    /**
     * Reade the byteBuffer(Obtained from lengthFieldDecoder), forward the request to the next handler.
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
        if (!(msg instanceof ByteBuf)) {
            throw new IllegalStateException("decode: Got illegal message " + msg);
        }
        // Decode the request type
        ByteBuf buf = (ByteBuf) msg;
        if (buf.readableBytes() < 5){
            return ;
        }
        //FIXME: without fixed frame decoder
        buf.markReaderIndex();
        int length = buf.readInt();
        if (buf.readableBytes() < length){
            if (logger.isDebugEnabled()){
                logger.debug("Expect bytes: {}, acutally {}", length, buf.readableBytes());
            }
            buf.resetReaderIndex();
            return ;
        }
        int enumValue = buf.readByte();
        if (logger.isDebugEnabled()){
            logger.debug("received buf size: {}, enum {}", length, enumValue);
        }
        RequestType type = RequestType.values()[enumValue];
        Class<? extends WritableRequest> messageClass = type.getClazz();

        logger.debug("Decoder {}-{}: message clz: {}, bytes to read {}", conf.getWorkerId(), decoderId, messageClass.getName(), buf.readableBytes());

        WritableRequest request = ReflectionUtils.newInstance(messageClass);
        //Conf contains class info to create message instance.
        request.setConf(conf);
        ByteBuf copied = buf.copy();
        if (logger.isDebugEnabled()){
            logger.debug("readerindex: " + copied.readerIndex());
        }
        request = RequestUtils.decodeWritableRequest(copied, request);
//        buf.retain();
        buf.release();
        logger.debug("decode res: {}", request);
        ctx.fireChannelRead(request);
    }

//    /**
//     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the
//     * input {@link ByteBuf} has nothing to read when return from this method or till nothing was
//     * read from the input {@link ByteBuf}.
//     *
//     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs
//     *            to
//     * @param in  the {@link ByteBuf} from which to read data
//     * @param out the {@link List} to which decoded messages should be added
//     * @throws Exception is thrown if an error occurs
//     */
//    @Override
//    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
//        throws Exception {
//        if (in.readableBytes() < 4 + 1) {
//            logger.warn("return since only "+ in.readableBytes() + " available, need at least 5");
//            return;
//        }
//        in.markReaderIndex();
//        //num of bytes
//        int numBytes = in.readInt();
//        if (numBytes < 0){
//            logger.error("Expect a positive number of bytes" + numBytes);
////            ctx.close();
//            return ;
//        }
//        if (in.readableBytes() < numBytes){
//            in.resetReaderIndex();
//            logger.error("Decoder [" + conf.getWorkerId() + "-" + decoderId + "] not enough num bytes: " + in.readableBytes() + " expected: " + numBytes + " times:" + waitingFullMsgTimes++);
//            return ;
//        }
//        logger.warn("Setting waiting full msg times from :" + waitingFullMsgTimes + " to 0");
//        waitingFullMsgTimes = 0;
//
//        // Decode the request type
//        int enumValue = in.readByte();
//        RequestType type = RequestType.values()[enumValue];
//        Class<? extends WritableRequest> messageClass = type.getClazz();
//
//        logger.debug(
//                "decode: Client "
//                    + messageClass.getName()
//                    + ", with size "
//                    + in.readableBytes());
//
//        WritableRequest request = ReflectionUtils.newInstance(messageClass);
//        //Conf contains class info to create message instance.
//        request.setConf(conf);
//        request = RequestUtils.decodeWritableRequest(in, request);
//        assert in.release();
//        logger.debug("decode res: " + request);
//        out.add(request);
//    }
}
