package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jnr.ffi.annotations.In;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexDataManager;
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
    private ImmutableClassesGiraphConfiguration conf;

    public VertexDataManagerImpl(SimpleFragment fragment, VertexRange<Long> vertices,
        ImmutableClassesGiraphConfiguration configuration) {
        this.fragment = fragment;
        this.vertices = vertices;
        this.maxVertexLid = vertices.end().GetValue();
        vertexDataList = new ArrayList<VDATA_T>((int) vertices.size());
        this.conf = configuration;

        FFIByteVectorInputStream inputStream = generateVertexDataStream();

        try {
            for (int i = 0; i < vertices.size(); ++i) {
                VDATA_T vdata = (VDATA_T) conf.createVertexValue();
                vdata.readFields(inputStream);
                vertexDataList.add(vdata);
            }
        } catch (IOException e) {
            e.printStackTrace();
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

    private FFIByteVectorInputStream generateVertexDataStream() {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        try {
            for (long i = 0; i < vertices.size(); ++i) {
                vertex.SetValue(i);
                if (conf.getGrapeVdataClass().equals(Long.class)) {
                    Long value = (Long) fragment.getData(vertex);
                    outputStream.writeLong(value);
                }
                else if (conf.getGrapeVdataClass().equals(Integer.class)){
                    Integer value = (Integer) fragment.getData(vertex);
                    outputStream.writeInt(value);
                }
                else if (conf.getGrapeVdataClass().equals(Double.class)){
                    Double value = (Double) fragment.getData(vertex);
                    outputStream.writeDouble(value);
                }
            }
            outputStream.finishSetting();
            logger.info("Vertex data stream size: " + outputStream.bytesWriten() + ", vertices: " + vertices.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FFIByteVectorInputStream(outputStream.getVector());
    }
}
