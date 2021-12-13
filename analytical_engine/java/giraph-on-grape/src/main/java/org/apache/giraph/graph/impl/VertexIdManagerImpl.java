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
import jnr.ffi.annotations.In;
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
    private VertexRange<Long> vertices;
    private List<OID_T> vertexIdList;
    private ImmutableClassesGiraphConfiguration conf;

    /**
     * To provide giraph users with all oids, we need to get all oids out of c++ memory, then let
     * java read the stream.
     *
     * @param fragment fragment
     * @param vertices vertex range covers
     * @param conf     configuration to use.
     */
    public VertexIdManagerImpl(SimpleFragment fragment, VertexRange<Long> vertices,
        ImmutableClassesGiraphConfiguration conf) {
        this.fragment = fragment;
        this.vertices = vertices;
        this.conf = conf;
        vertexIdList = new ArrayList<OID_T>((int) vertices.size());

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        FFIByteVectorInputStream inputStream = generateVertexIdStream();

        Class<?> grapeOidClass = conf.getGrapeOidClass();
        try {
            for (int i = 0; i < vertices.size(); ++i) {
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
            for (long i = 0; i < vertices.size(); ++i) {
                vertex.SetValue(i);
                if (conf.getGrapeOidClass().equals(Long.class)) {
                    Long value = (Long) fragment.getId(vertex);
                    outputStream.writeLong(value);
                }
                else if (conf.getGrapeOidClass().equals(Integer.class)){
                    Integer value = (Integer) fragment.getId(vertex);
                    outputStream.writeInt(value);
                }
                else {
                    logger.error("Unsupported oid class: " + conf.getGrapeOidClass().getName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FFIByteVectorInputStream(outputStream.getVector());
    }
}
