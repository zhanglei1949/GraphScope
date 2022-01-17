package com.alibaba.graphscope.parallel.netty.request.impl;

import com.alibaba.graphscope.parallel.message.LongDoubleMessageStore;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A special type of request which store byteBuf as store.
 */
public class ByteBufRequest extends WritableRequest {
    private static Logger logger = LoggerFactory.getLogger(ByteBufRequest.class);

    private ByteBuf buf;

    public ByteBufRequest(ByteBuf in) {
        buf = in;
    }

    /**
     * Default constructor for reflection usage. SetBuffer should be called after construction.
     */
    public ByteBufRequest() {

    }

    /**
     * Serialization of request type is taken care by encoder.
     *
     * @return request type.
     */
    @Override
    public RequestType getRequestType() {
        return RequestType.BYTEBUF_REQUEST;
    }

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void writeFieldsRequest(DataOutput output) throws IOException {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public int getNumBytes() {
        if (Objects.nonNull(buf)) {
            return buf.readableBytes();
        }
        return 0;
    }

    /**
     * @param buf
     */
    @Override
    public void setBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public ByteBuf getBuffer() {
        return buf;
    }

    /**
     * Apply this request on this message storage.
     *
     * @param messageStore message store.
     */
    @Override
    public void doRequest(MessageStore messageStore) {
        if (Objects.isNull(buf)) {
            throw new IllegalStateException("try to do request on an empty byteBuf request");
        }
        if (messageStore instanceof LongDoubleMessageStore) {
            if (buf.readableBytes() % 16 != 0) {
                throw new IllegalStateException("readable bytes" + buf.readableBytes() +" can not be subtracted by 16");
            }
            LongDoubleMessageStore longDoubleMessageStore = (LongDoubleMessageStore) messageStore;
            DoubleWritable writable = new DoubleWritable();
            while (buf.isReadable(16)) {
                long gid = buf.readLong();
                double msg = buf.readDouble();
                if (logger.isDebugEnabled()){
                    logger.debug("worker [{}] doRequest: feeding msg to message store: gid [{}], msg [{}]", getConf().getWorkerId(),gid, msg);
                }
                writable.set(msg);
                longDoubleMessageStore.addGidMessage(gid, writable);
            }
            assert buf.readableBytes() == 0;
            //release buf here?
            buf.release(2);
            if (buf.refCnt() > 0){
                throw new IllegalStateException("not released: " + buf);
            }
        } else {
            throw new IllegalStateException("Not available mesageStore" + messageStore);
        }
    }

    @Override
    public String toString() {
        return "ByteBufRequest(size=" + buf.readableBytes() + ")";
    }
}
