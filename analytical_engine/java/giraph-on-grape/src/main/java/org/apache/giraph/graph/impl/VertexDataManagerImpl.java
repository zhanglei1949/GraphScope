package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.WritableFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Resolve the vertex data getting issue.
 *
 * @param <VDATA_T> vertex data type.
 */
public class VertexDataManagerImpl<VDATA_T extends Writable> implements VertexDataManager<VDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(VertexDataManagerImpl.class);

    private SimpleFragment fragment;
    private VertexRange<Long> vertices;
    private List<VDATA_T> vertexDataList;
    private long maxVertexLid;

    public VertexDataManagerImpl(SimpleFragment fragment, VertexRange<Long> vertices) {
        this.fragment = fragment;
        this.vertices = vertices;
	this.maxVertexLid = vertices.end().GetValue();
        vertexDataList = new ArrayList<VDATA_T>((int) vertices.size());
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (int i = 0; i < vertices.size(); ++i) {
            vertex.SetValue((long) i);
//            vertexDataList.add((VDATA_T) new LongWritable((Long) this.fragment.getData(vertex)));
            VDATA_T vdata = (VDATA_T) WritableFactory.newVData();
            //TODO: in the future vdata should be read from stream. Currently we use hacky method for test
            if (vdata instanceof DoubleWritable){
                ((DoubleWritable) vdata).set((Double) this.fragment.getData(vertex));
            }
            else if (vdata instanceof LongWritable){
                ((LongWritable) vdata).set((Long) this.fragment.getData(vertex));
            }
            vertexDataList.add(vdata);
        }
    }

    @Override
    public VDATA_T getVertexData(long lid) {
        checkLid(lid);
        return vertexDataList.get((int) lid);
    }

    @Override
    public void setVertexData(long lid, VDATA_T vertexData) {
        checkLid(lid);
        vertexDataList.set((int) lid, vertexData);
    }

    private void checkLid(long lid) {
        if (lid >= maxVertexLid) {
            logger.error("Querying lid out of range: " + lid + " max lid: " + lid);
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }
}
