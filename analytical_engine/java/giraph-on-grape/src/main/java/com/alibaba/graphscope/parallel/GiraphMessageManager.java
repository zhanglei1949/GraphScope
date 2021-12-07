package com.alibaba.graphscope.parallel;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface GiraphMessageManager<OID_T extends WritableComparable,VDATA_T extends Writable, EDATA_T extends Writable, IN_MSG_T extends Writable, OUT_MSG_T extends Writable> {

    /**
     * Called by our framework, to deserialize the messages from c++ to java.
     * Must be called before getMessages
     */
    void receiveMessages();

    /**
     * Get the messages received from last round.
     * @param lid local id.
     * @return received msg.
     */
    Iterable<IN_MSG_T> getMessages(long lid);

    /**
     * Send msg to all neighbors of vertex.
     * @param vertex querying vertex
     * @param message message to send.
     */
    void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message);

    /**
     * Make sure all messages has been sent.
     */
    void finishMessageSending();
}
