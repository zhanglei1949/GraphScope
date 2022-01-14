package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vertex data type = double
 * @param <VDATA_T>
 * @param <GRAPE_OID_T>
 * @param <GRAPE_VID_T>
 * @param <GRAPE_VDATA_T>
 * @param <GRAPE_EDATA_T>
 */
public class LongVidDoubleVertexDataManagerImpl<VDATA_T extends Writable,GRAPE_OID_T,GRAPE_VID_T,GRAPE_VDATA_T,GRAPE_EDATA_T> implements
    VertexDataManager<VDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(LongVidDoubleVertexDataManagerImpl.class);

    private SimpleFragment<GRAPE_OID_T,GRAPE_VID_T,GRAPE_VDATA_T,GRAPE_EDATA_T> fragment;
    private long vertexNum;
    private ImmutableClassesGiraphConfiguration<?, VDATA_T,?> conf;
    private Vertex<Long> grapeVertex;

    public LongVidDoubleVertexDataManagerImpl(SimpleFragment<GRAPE_OID_T,GRAPE_VID_T,GRAPE_VDATA_T,GRAPE_EDATA_T> fragment, long vertexNum,
        ImmutableClassesGiraphConfiguration<?,VDATA_T,?> configuration) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        this.conf = configuration;

        if (!conf.getGrapeVidClass().equals(Long.class) || !conf.getGrapeVdataClass().equals(Double.class)){
            throw new IllegalStateException("Expect fragment with long vid and double vdata");
        }
        grapeVertex = FFITypeFactoryhelper.newVertex(Long.class);
        grapeVertex.SetValue(0L);
    }

    @Override
    public VDATA_T getVertexData(long lid) {
        checkLid(lid);
        grapeVertex.SetValue(lid);
        double vertexData = (double) fragment.getData((Vertex<GRAPE_VID_T>) grapeVertex);
        return (VDATA_T) new DoubleWritable(vertexData);
    }

    @Override
    public void setVertexData(long lid, VDATA_T vertexData) {
        throw new IllegalStateException("Not implemented");
    }

    private void checkLid(long lid) {
        if (lid >= vertexNum) {
            logger.error("Querying lid out of range: " + lid + " max lid: " + lid);
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }
}
