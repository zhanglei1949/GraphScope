package com.alibaba.graphscope.parallel.netty.request;

import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.message.InMessageStore;
import org.apache.hadoop.io.WritableComparable;

/**
 * Sub class requests implement this interface can send message to itself, locally.
 */
public interface RequestToSelf {

    /**
     * When a message is to self, we can avoid sending via network, and directly add to local cache
     * @param messageCache where this worker store local in messages.
     */
    void sendRequestToSelf(InMessageStore messageCache);
}
