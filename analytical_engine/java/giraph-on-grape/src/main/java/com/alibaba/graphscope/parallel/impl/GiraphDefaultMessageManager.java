package com.alibaba.graphscope.parallel.impl;

import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.BitSet;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class GiraphDefaultMessageManager<OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable, IN_MSG_T extends Writable, OUT_MSG_T extends Writable> implements
    GiraphMessageManager<OID_T,VDATA_T,EDATA_T,IN_MSG_T,OUT_MSG_T> {
    private SimpleFragment fragment;
    private DefaultMessageManager grapeMessageManager;
    private BitSet bitSet;
    private com.alibaba.graphscope.ds.Vertex<Long> grapeVertex;

    public GiraphDefaultMessageManager(SimpleFragment fragment, DefaultMessageManager defaultMessageManager){
        this.fragment = fragment;
        this.grapeMessageManager = defaultMessageManager;
        bitSet = new BitSet();
        grapeVertex = FFITypeFactoryhelper.newVertexLong();
    }

    @Override
    public Iterable<IN_MSG_T> getMessages(Vertex<OID_T,VDATA_T,EDATA_T> vertex) {
        return null;
    }

    /**
     * Send message to neighbor vertices.
     * @param vertex
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T,VDATA_T,EDATA_T> vertex, OUT_MSG_T message) {
        VertexImpl<OID_T,VDATA_T,EDATA_T> vertexImpl = (VertexImpl<OID_T, VDATA_T, EDATA_T>) vertex;
        bitSet.set((int)vertexImpl.getLocalId());
        grapeVertex.SetValue(vertexImpl.getLocalId());
//        this.grapeMessageManager.sendMsgThroughEdges(fragment, grapeVertex, );
    }
}
