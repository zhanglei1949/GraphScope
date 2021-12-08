package org.apache.giraph.graph;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.fragment.SimpleFragment;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VertexFactory {

    public static <OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable> Vertex<OID_T, VDATA_T, EDATA_T> createDefaultVertex(
        SimpleFragment fragment, GiraphComputationAdaptorContext context) {
        return new VertexImpl<OID_T, VDATA_T,EDATA_T>(fragment, context);
    }
}
