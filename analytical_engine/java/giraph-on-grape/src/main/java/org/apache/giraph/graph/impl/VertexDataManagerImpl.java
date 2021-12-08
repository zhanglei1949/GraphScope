package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class VertexDataManagerImpl<VDATA_T extends Writable> implements VertexDataManager<VDATA_T> {
    private SimpleFragment fragment;
    private VertexRange<Long> vertices;
    private List<VDATA_T> vertexDataList;

    public VertexDataManagerImpl(SimpleFragment fragment, VertexRange<Long> vertices){
        this.fragment = fragment;
        this.vertices = vertices;
        vertexDataList = new ArrayList<VDATA_T>((int)vertices.size());
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (int i = 0; i < vertices.size(); ++i){
            vertex.SetValue((long)i);
            vertexDataList.add((VDATA_T) new LongWritable((Long) this.fragment.getData(vertex)));
        }
    }

    @Override
    public VDATA_T getVertexData(long lid) {
        return vertexDataList.get((int)lid);
    }

    @Override
    public void setVertexData(long lid, VDATA_T vertexData) {
        vertexDataList.set((int) lid, vertexData);
    }
}
