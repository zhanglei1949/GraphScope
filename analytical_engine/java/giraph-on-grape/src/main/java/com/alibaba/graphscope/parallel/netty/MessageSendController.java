package com.alibaba.graphscope.parallel.netty;

import com.alibaba.graphscope.parallel.netty.request.WritableRequest;

/**
 * Interface defining a controller which is responsible for controlling the message sending strategies.
 * The actual sending is not done by us, is done by Netty Client.
 */
public interface MessageSendController {
    void sendRequest(int workerId, WritableRequest request);
}
