package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized implementation for long oid. Don't store values, create obj when needed.
 *
 * @param <OID_T>              original vertex id
 * @param <GRAPE_OID_T>        grape oid
 * @param <GRAPE_VID_T>        grape vid
 * @param <GRAPE_VDATA_T>      grape vdata
 * @param <GRAPE_EDATA_T>grape edata
 */
public class LongVidLongVertexIdManagerImpl<OID_T extends WritableComparable, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> implements
    VertexIdManager<OID_T> {

    private static Logger logger = LoggerFactory.getLogger(LongVidLongVertexIdManagerImpl.class);

    private SimpleFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private long vertexNum;
    //    private List<OID_T> vertexIdList;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;
    private Vertex<Long> grapeVertex;


    public LongVidLongVertexIdManagerImpl(
        SimpleFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
        long vertexNum,
        ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.vertexNum = vertexNum; //fragment vertex Num
        this.conf = conf;

        if (!conf.getGrapeVidClass().equals(Long.class)) {
            throw new IllegalStateException(
                "LongVertexIdManager expect the fragment using long as oid");
        }

        grapeVertex = FFITypeFactoryhelper.newVertex(Long.class);
        grapeVertex.SetValue(0L);
    }

    @Override
    public OID_T getId(long lid) {
        checkLid(lid);
        grapeVertex.SetValue(lid);
        Long oid = (Long) fragment.getId((Vertex<GRAPE_VID_T>) grapeVertex);
        return (OID_T) new LongWritable(oid);
    }

    private void checkLid(long lid) {
        if (lid >= vertexNum) {
            logger.error("Querying lid out of range: " + lid + " max lid: " + lid);
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }
}
