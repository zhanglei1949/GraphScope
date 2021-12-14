package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The access of giraph oids shall be managed by this class.
 *
 * @param <OID_T> original id type.
 */
public class VertexIdManagerImpl<OID_T extends WritableComparable> implements
    VertexIdManager<OID_T> {

    private static Logger logger = LoggerFactory.getLogger(VertexIdManagerImpl.class);

    private SimpleFragment fragment;
    private long vertexNum;
    private List<OID_T> vertexIdList;
    private ImmutableClassesGiraphConfiguration conf;

    /**
     * To provide giraph users with all oids, we need to get all oids out of c++ memory, then let
     * java read the stream.
     *
     * @param fragment  fragment
     * @param vertexNum number of vertices
     * @param conf      configuration to use.
     */
    public VertexIdManagerImpl(SimpleFragment fragment, long vertexNum,
        ImmutableClassesGiraphConfiguration conf) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        this.conf = conf;
        vertexIdList = new ArrayList<OID_T>((int) vertexNum);

        FFIByteVectorInputStream inputStream = generateVertexIdStream();
        try {
            for (int i = 0; i < vertexNum; ++i) {
                WritableComparable oid = conf.createVertexId();
                oid.readFields(inputStream);
                vertexIdList.add((OID_T) oid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        inputStream.clear();
    }

    @Override
    public OID_T getId(long lid) {
        return vertexIdList.get((int) lid);
    }

    private FFIByteVectorInputStream generateVertexIdStream() {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        try {
            if (conf.getGrapeOidClass().equals(Long.class)) {
                for (long i = 0; i < vertexNum; ++i) {
                    vertex.SetValue(i);
                    Long value = (Long) fragment.getId(vertex);
                    outputStream.writeLong(value);
                }
            } else if (conf.getGrapeOidClass().equals(Integer.class)) {
                for (long i = 0; i < vertexNum; ++i) {
                    vertex.SetValue(i);
                    Integer value = (Integer) fragment.getId(vertex);
                    outputStream.writeInt(value);
                }
            }
            // if grape oid class is user defined class
            else {
                logger.error("Unsupported oid class: " + conf.getGrapeOidClass().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FFIByteVectorInputStream(outputStream.getVector());
    }
}
