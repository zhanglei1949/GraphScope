package com.alibaba.graphscope.parallel.cache;

import com.alibaba.graphscope.parallel.message.MessageStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Message cache is a special case of SendDataCache, in which the stored data is a pair (I,D),I is
 * the vertex OID, D is the message for Vertex.
 */
public interface SendMessageCache<I extends WritableComparable,
    M extends Writable, GS_VID_T> {

    void sendMessage(int dstFragId, GS_VID_T gid, M message);

    void removeMessageToSelf(MessageStore<I,M,GS_VID_T> nextIncomingMessages);
    /**
     * FLush all cached messages out.
     */
    void flushMessage();

    void clear();

}
