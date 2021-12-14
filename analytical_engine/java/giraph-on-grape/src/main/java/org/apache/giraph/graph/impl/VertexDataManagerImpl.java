package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    private List<VDATA_T> vertexDataList;
    private long vertexNum;
    private ImmutableClassesGiraphConfiguration conf;

    public VertexDataManagerImpl(SimpleFragment fragment, long vertexNum,
        ImmutableClassesGiraphConfiguration configuration) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        vertexDataList = new ArrayList<VDATA_T>((int) vertexNum);
        this.conf = configuration;

        FFIByteVectorInputStream inputStream = generateVertexDataStream();

        try {
            for (int i = 0; i < vertexNum; ++i) {
                VDATA_T vdata = (VDATA_T) conf.createVertexValue();
                vdata.readFields(inputStream);
                vertexDataList.add(vdata);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        inputStream.clear();
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
        if (lid >= vertexNum) {
            logger.error("Querying lid out of range: " + lid + " max lid: " + lid);
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }

    private FFIByteVectorInputStream generateVertexDataStream() {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        try {
            //We need to form all vdata as a stream, so java writables can read from this stream.
            if (conf.getGrapeVdataClass().equals(Long.class)) {
                for (long i = 0; i < vertexNum; ++i) {
                    vertex.SetValue(i);
                    Long value = (Long) fragment.getData(vertex);
                    outputStream.writeLong(value);
                }
            } else if (conf.getGrapeVdataClass().equals(Integer.class)) {
                for (long i = 0; i < vertexNum; ++i) {
                    vertex.SetValue(i);
                    Integer value = (Integer) fragment.getData(vertex);
                    outputStream.writeInt(value);
                }
            }
            else if (conf.getGrapeVdataClass().equals(Double.class)){
                for (long i = 0; i < vertexNum; ++i) {
                    vertex.SetValue(i);
                    Double value = (Double) fragment.getData(vertex);
                    outputStream.writeDouble(value);
                }
            }
            //else if (conf.getGrapeVdataClass().equals the userDefined class...
            outputStream.finishSetting();
            logger.info("Vertex data stream size: " + outputStream.bytesWriten() + ", vertices: " + vertexNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FFIByteVectorInputStream(outputStream.getVector());
    }
}
