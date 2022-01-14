package com.alibaba.graphscope.parallel.cache.impl;

import static org.apache.giraph.conf.GiraphConstants.MAX_OUT_MSG_CACHE_SIZE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.message.LongDoubleMessageStore;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import com.alibaba.graphscope.parallel.netty.request.impl.ByteBufRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use a byteBuf as underlying cache storage.
 * @param <I> vertex id type
 * @param <M> message type
 * @param <GS_VID_T> gs vid
 */
public class ByteBufMessageCache <I extends WritableComparable,
    M extends Writable, GS_VID_T> implements SendMessageCache<I,M,GS_VID_T> {
    private static Logger logger = LoggerFactory.getLogger(ByteBufMessageCache.class);

    private final int fragNum;
    private final int fragId;
    private final NettyClient client;
    private final ImmutableClassesGiraphConfiguration<I,?,?> conf;
    private ByteBuf[] cache;
    private ByteBufOutputStream cacheStream [];
    private int cacheMaximum;

    public ByteBufMessageCache(int fragNum, int fragId, NettyClient client, ImmutableClassesGiraphConfiguration<I,?,?> conf) {
        this.fragNum = fragNum;
        this.fragId = fragId;
        this.client = client;
        this.conf = conf;
        //Ideally, this buffer should be able to consumed by ByteBufRequest without any copy, so we
        //reserve space for headers.
        cacheMaximum = MAX_OUT_MSG_CACHE_SIZE.get(conf) + SIZE_OF_INT + SIZE_OF_BYTE;

//        int cacheSize = MAX_OUT_MSG_CACHE_SIZE.get(conf);
        logger.info("ByteBuf buf setting cache size to [{}]", cacheMaximum);
        cache = new ByteBuf[fragNum];
        cacheStream = new ByteBufOutputStream[fragNum];

        for (int i = 0; i < fragNum; ++i){
            cache[i] = conf.getNettyAllocator().buffer(cacheMaximum);
            cacheStream[i] = new ByteBufOutputStream(cache[i]);
            cache[i].writeInt(0);
            cache[i].writeByte(0);
        }
    }

    @Override
    public void sendMessage(int dstFragId, GS_VID_T gid, M message) {
        if (cache[dstFragId].readableBytes() >= cacheMaximum){
            logger.info("Cache limit reached, flushing buffer");
            ByteBufRequest request = new ByteBufRequest(cache[dstFragId]);
            // the data in this buffer [cache] will be flushed to netty cache.
            client.sendMessage(dstFragId, request);
            //don't need to create new cache, just reset the cache.
            cache[dstFragId].clear();
        }
        try{
            if (logger.isDebugEnabled()){
                logger.debug("worker [{}]: send msg to worker [{}], dstGid {}, msg {}", fragId, dstFragId, gid, message);
            }
            cacheStream[dstFragId].writeLong((Long) gid);
            message.write(cacheStream[dstFragId]);
        }
        catch (Exception e){
            e.printStackTrace();
            throw new IllegalStateException("Exception in sending msg");
        }
    }

    @Override
    public void removeMessageToSelf(MessageStore<I, M, GS_VID_T> nextIncomingMessages) {
        //move toSelf msg to messageStore
        if (cache[fragId].readableBytes() > 0){
            if (nextIncomingMessages instanceof LongDoubleMessageStore){
                LongDoubleMessageStore doubleMessageStore = (LongDoubleMessageStore) nextIncomingMessages;
                //DoubleMessageStore should copy this memory.
                doubleMessageStore.digestByteBuf(cache[fragId]);
            }
            else {
                throw new IllegalStateException("Not supported now");
            }
        }
//        cache[fragId].clear();
//        cache[fragId].writeInt(0);
//        cache[fragId].writeByte(0);
    }

    /**
     * FLush all cached messages out.
     */
    @Override
    public void flushMessage() {
        sendCurrentMessageInCache();
        client.waitAllRequests();
    }

    @Override
    public void clear() {
        for (int i = 0; i < fragNum; ++i){
            cache[i].clear();
            cache[i].writeInt(0);
            cache[i].writeByte(0);
            //no need to clear output stream, since writeIndex is set to 0 for byteBuf.
        }
    }

    private void sendCurrentMessageInCache(){
        for (int dstFragId = 0; dstFragId < cache.length; ++dstFragId){
            if (logger.isDebugEnabled()){
                logger.debug("worker {} to {}, cache {}", fragId, dstFragId, cache[dstFragId]);
            }
            if (dstFragId != fragId && cache[dstFragId].readableBytes() > 0){
                ByteBufRequest request = new ByteBufRequest(cache[dstFragId]);
                logger.info("worker [{}] flush buffered msg of size [{}] to worker [{}]", fragId, cache[dstFragId].readableBytes(), dstFragId);
                client.sendMessage(dstFragId,request);
//                cache[dstFragId].clear();
//                cache[dstFragId].writeInt(0);
//                cache[dstFragId].writeByte(0);
            }
        }
        logger.info("frag [{}] finish flushing cache", fragId);
    }
}
