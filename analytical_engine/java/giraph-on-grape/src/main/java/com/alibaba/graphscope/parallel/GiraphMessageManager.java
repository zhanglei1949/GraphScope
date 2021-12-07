package com.alibaba.graphscope.parallel;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface GiraphMessageManager<OID_T extends WritableComparable,VDATA_T extends Writable, EDATA_T extends Writable, IN_MSG_T extends Writable, OUT_MSG_T extends Writable> {
    Iterable<IN_MSG_T> getMessages(Vertex<OID_T, VDATA_T,EDATA_T> vertex);

    void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message);
}
