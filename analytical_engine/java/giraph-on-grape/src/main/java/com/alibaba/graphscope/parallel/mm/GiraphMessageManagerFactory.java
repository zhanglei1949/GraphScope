package com.alibaba.graphscope.parallel.mm;

import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.mm.impl.GiraphDefaultMessageManager;
import com.alibaba.graphscope.parallel.mm.impl.GiraphNettyMessageManager;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GiraphMessageManagerFactory {

    private static Logger logger = LoggerFactory.getLogger(GiraphMessageManagerFactory.class);

    /**
     *
     * @param mmType netty or mpi,
     * @param fragment grape fragment
     * @param grapeMessager used by mpi, DefaultMessageManager
     * @param networkMap used by netty
     * @param conf configuration
     * @return
     */
    public static GiraphMessageManager create(String mmType, SimpleFragment fragment,
        DefaultMessageManager grapeMessager, NetworkMap networkMap, ImmutableClassesGiraphConfiguration conf) {
        //TODO: get ip or address from mpi.
//        NetworkMap networkMap = new NetworkMap(conf.getWorkerId(), conf.getWorkerNum(),
//            conf.getInitServerPort(), new String[]{"1"});
        if (mmType.equals("netty")) {
            return createGiraphNettyMM(fragment, networkMap, conf, conf.getVertexIdClass(), conf
                .getVertexValueClass(), conf.getEdgeValueClass(), conf
                .getIncomingMessageValueClass(), conf.getOutgoingMessageValueClass());
        } else if (mmType.equals("mpi")) {
            return createGiraphDefaultMM(fragment, grapeMessager, conf,conf.getVertexIdClass(), conf
                .getVertexValueClass(), conf.getEdgeValueClass(), conf
                .getIncomingMessageValueClass(), conf.getOutgoingMessageValueClass());
        }
        else{
            logger.error("Unrecognized message manager type: [" + mmType + "]");
            return null;
        }
    }

    private static <OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable, IN_MSG_T extends Writable, OUT_MSG_T extends Writable>
    GiraphNettyMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T> createGiraphNettyMM(
        SimpleFragment fragment,
        NetworkMap networkMap,
        ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf,
        Class<? extends OID_T> oidClass,
        Class<? extends VDATA_T> vdataClass,
        Class<? extends EDATA_T> edataClass,
        Class<? extends IN_MSG_T> inMsgClass,
        Class<? extends OUT_MSG_T> outMsgClass) {
        return new GiraphNettyMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T>(fragment,
            networkMap, conf);
    }

    private static <OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable, IN_MSG_T extends Writable, OUT_MSG_T extends Writable>
    GiraphDefaultMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T> createGiraphDefaultMM(
        SimpleFragment fragment,
        DefaultMessageManager mm,
        ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf,
        Class<? extends OID_T> oidClass,
        Class<? extends VDATA_T> vdataClass,
        Class<? extends EDATA_T> edataClass,
        Class<? extends IN_MSG_T> inMsgClass,
        Class<? extends OUT_MSG_T> outMsgClass) {
        return new GiraphDefaultMessageManager<OID_T,VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T>(fragment,mm
            , conf);
    }


}
