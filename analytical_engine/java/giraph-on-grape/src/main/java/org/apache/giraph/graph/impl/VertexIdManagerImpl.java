package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The access of giraph oids shall be managed by this class.
 * @param <OID_T> original id type.
 */
public class VertexIdManagerImpl<OID_T extends WritableComparable> implements VertexIdManager<OID_T> {
    private SimpleFragment fragment;
    private VertexRange<Long> vertices;
    private List<OID_T> vertexIdList;

    public VertexIdManagerImpl(SimpleFragment fragment, VertexRange<Long> vertices){
        this.fragment = fragment;
        this.vertices = vertices;
        vertexIdList = new ArrayList<OID_T>((int)vertices.size());
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (int i = 0; i < vertices.size(); ++i){
            vertex.SetValue((long)i);
            vertexIdList.add((OID_T) new LongWritable((Long) fragment.getId(vertex)));
        }
    }

    @Override
    public OID_T getId(long lid) {
        return vertexIdList.get((int) lid);
    }
}
