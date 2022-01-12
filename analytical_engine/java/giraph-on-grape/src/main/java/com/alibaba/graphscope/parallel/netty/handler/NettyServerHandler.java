package com.alibaba.graphscope.parallel.netty.handler;

import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.mm.impl.GiraphDefaultMessageManager;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServerHandler<OID_T extends WritableComparable,GS_VID_T> extends SimpleChannelInboundHandler<WritableRequest> {

    private MessageStore<OID_T, Writable,GS_VID_T> nextIncomingMessages;
    private SimpleFragment<?,GS_VID_T,?,?> fragment;
    private static Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);
    private int msgSeq;
//    private ByteBuf buf;


    public NettyServerHandler(SimpleFragment<?,GS_VID_T,?,?> fragment, MessageStore<OID_T,Writable,GS_VID_T> nextIncomingMessages){
        this.fragment = fragment;
        this.nextIncomingMessages = nextIncomingMessages;
//        this.msgSeq = new AtomicInteger(0);
        this.msgSeq = 0;
//        buf = new PooledByteBufAllocator(true).buffer(4);
    }

    /**
     * <strong>Please keep in mind that this method will be renamed to
     * {@code messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
     * <p>
     * Is called for each message of type {@link WritableRequest}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
     *            belongs to
     * @param msg the message to handle
     * @throws Exception is thrown if an error occurred
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WritableRequest msg) throws Exception {
        logger.debug("Server handler [" + fragment.fid() + "] thread: " + Thread.currentThread().getId() + " received msg: " + msg);
        msg.doRequest(nextIncomingMessages);

//        int curMsgSeq = msgSeq.getAndAdd(1);
        ByteBuf buf = ctx.alloc().buffer(4);
        buf.writeInt(msgSeq);
        logger.debug("Server handler[ " + fragment.fid() + " ] send response " + msgSeq);
        ctx.writeAndFlush(buf);
        msgSeq += 1;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    public void preSuperStep(MessageStore<OID_T, Writable,GS_VID_T> nextIncomingMessages){
        logger.info("Update nextIncoming msg store from " + this.nextIncomingMessages + " to " + nextIncomingMessages);
        this.nextIncomingMessages = nextIncomingMessages;
        this.msgSeq = 0;
    }
}
