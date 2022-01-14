package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specialized store for double msgs.
 *
 * @param <OID_T>
 */
public class LongDoubleMessageStore<OID_T extends WritableComparable> implements
    MessageStore<OID_T, DoubleWritable, Long> {

    private static Logger logger = LoggerFactory.getLogger(LongDoubleMessageStore.class);
    private static int INIT_CAPACITY = 2;

    private SimpleFragment<?, Long, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;
    private Vertex<Long> vertex;
    private Map<Long, List<Double>> messages;
    private DoubleWritableIterable iterable;


    public LongDoubleMessageStore(SimpleFragment fragment,
        ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = (Vertex<Long>) FFITypeFactoryhelper.newVertex(java.lang.Long.class);
        iterable = new DoubleWritableIterable();
        messages = new HashMap<Long,List<Double>>((int) fragment.getInnerVerticesNum());
    }

    @Override
    public void addLidMessage(Long lid, DoubleWritable writable) {
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>(INIT_CAPACITY));
        }
        messages.get(lid).add(writable.get());
    }

    @Override
    public void addGidMessages(Iterator<Long> gidIterator,
        Iterator<DoubleWritable> writableIterator) {
        int cnt = 0;
        while (gidIterator.hasNext() && writableIterator.hasNext()) {
            long gid = gidIterator.next();
            DoubleWritable msg = writableIterator.next();
            boolean res = fragment.gid2Vertex(gid, vertex);
            long lid = vertex.GetValue();
            if (!res) {
                throw new IllegalStateException("convert gid: " + gid + " to lid failed: ");
            }

            if (!messages.containsKey(lid)) {
                messages.put(lid, new ArrayList<>());
            }
            messages.get(vertex.GetValue()).add(msg.get());
            cnt += 1;
        }
        logger.info("worker [{}] messages to self cnt: {}", fragment.fid(), cnt);
    }

    /**
     * For messages bound with gid, first get lid.
     *
     * @param gid      global id
     * @param writable msg
     */
    @Override
    public synchronized void addGidMessage(Long gid, DoubleWritable writable) {
        addGidMessage(gid, writable.get());
    }

    private synchronized void addGidMessage(Long gid, double msg){
        vertex.SetValue(gid);
        assert fragment.gid2Vertex(gid, vertex);
        long lid = vertex.GetValue();
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>());
        }
        messages.get(lid).add(msg);
    }

    /**
     * For input byteBuf, parse and update our store.
     *
     * The received buf contains 4+1+data.
     * @param buf
     */
    public void digestByteBuf(ByteBuf buf){
        ByteBuf bufCopy = buf.copy();
        bufCopy.skipBytes(5);
        if (bufCopy.readableBytes() % 16 != 0){
            throw new IllegalStateException("Expect number of bytes times of 16");
        }
        logger.info("LongDoubleMsgStore digest bytebuf size {} direct {}", bufCopy.readableBytes(), bufCopy.isDirect());
        while (bufCopy.readableBytes() >= 16){
            long gid = bufCopy.readLong();
            double msg = bufCopy.readDouble();
            addGidMessage(gid, msg);
        }
        assert bufCopy.readableBytes() == 0;
        assert bufCopy.release();
    }

    @Override
    public void swap(MessageStore<OID_T, DoubleWritable, Long> other) {
        if (other instanceof LongDoubleMessageStore) {
            LongDoubleMessageStore<OID_T> longDoubleMessageStore = (LongDoubleMessageStore<OID_T>) other;
            if (!this.fragment.equals(longDoubleMessageStore.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Map<Long, List<Double>> tmp;
            tmp = this.messages;
            this.messages = longDoubleMessageStore.messages;
            longDoubleMessageStore.messages = tmp;
        } else {
            logger.error("Can not swap with a non-longDoubleMessageStore obj");
        }
    }

    @Override
    public void clearAll() {
        messages.clear();
    }

    /**
     * Check whether any messages received.
     */
    @Override
    public boolean anyMessageReceived() {
        return !messages.isEmpty();
    }

    /**
     * Check for lid, any messages available.
     *
     * @param lid lid.
     * @return true if has message
     */
    @Override
    public boolean messageAvailable(long lid) {
        return messages.containsKey(lid);
    }

    /**
     * Avoid creating messages, this function is not thread-safe.
     *
     * @param lid
     * @return
     */
    @Override
    public Iterable<DoubleWritable> getMessages(long lid) {
        if (messages.containsKey(lid)) {
            iterable.init(messages.get(lid));
            return iterable;
        } else {
            //actually a static empty iterator.
            return () -> Collections.emptyIterator();
        }
    }

    public static class DoubleWritableIterable implements Iterable<DoubleWritable> {

        private List<Double> doubles;
        private int ind;
        private DoubleWritable writable;

        public DoubleWritableIterable() {
            doubles = new ArrayList<>();
            ind = 0;
        }

        public void init(List<Double> in) {
            doubles = in;
            ind = 0;
            writable = new DoubleWritable();
        }

        @Override
        public Iterator<DoubleWritable> iterator() {
            return new Iterator<DoubleWritable>() {

                @Override
                public boolean hasNext() {
                    return ind < doubles.size();
                }

                @Override
                public DoubleWritable next() {
                    writable.set(doubles.get(ind++));
                    return writable;
                }
            };
        }
    }
}
