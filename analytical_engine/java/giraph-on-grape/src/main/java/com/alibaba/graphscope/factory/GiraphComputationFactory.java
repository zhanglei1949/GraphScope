package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.app.GiraphComputationAdaptor;
import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.graph.impl.ImmutableEdgeManagerImpl;
import org.apache.giraph.graph.impl.LongVidDoubleVertexDataManagerImpl;
import org.apache.giraph.graph.impl.LongVidLongVertexIdManagerImpl;
import org.apache.giraph.graph.impl.VertexDataManagerImpl;
import org.apache.giraph.graph.impl.VertexIdManagerImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class GiraphComputationFactory {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationFactory.class);

    /**
     * Create the giraph computation adaptor for the input fragment. Use fragment's actual type
     * parameters to initiate a generic adaptor.
     *
     * @param className adaptor class name
     * @param fragment simple fragment, which is parameterized.
     * @return created adaptor.
     */
    public static <OID_T, VID_T, VDATA_T, EDATA_T>
            GiraphComputationAdaptor createGiraphComputationAdaptor(
                    String className,
                    ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        Class<?>[] classes = getTypeArgumentFromInterface(IFragment.class, fragment.getClass());
        if (classes.length != 4) {
            logger.error("Expected 4 type params, parsed: {}", classes.length);
            return null;
        }
        return createGiraphComputationAdaptor(classes[0], classes[1], classes[2], classes[3]);
    }

    /**
     * Create the giraph computation adaptor context for the input fragment. Use fragment's actual
     * type parameters to initiate a generic adaptor context.
     *
     * @param className adaptor class name
     * @param fragment simple fragment, which is parameterized.
     * @return created adaptor.
     */
    public static <OID_T, VID_T, VDATA_T, EDATA_T>
            GiraphComputationAdaptorContext createGiraphComputationAdaptorContext(
                    String className,
                    ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> fragment) {
        Class<?>[] classes = getTypeArgumentFromInterface(IFragment.class, fragment.getClass());
        if (classes.length != 4) {
            logger.error("Expected 4 type params, parsed: {}", classes.length);
            return null;
        }
        return createGiraphComputationAdaptorContext(
                classes[0], classes[1], classes[2], classes[3]);
    }

    public static <VDATA_T extends Writable, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
            VertexDataManager<VDATA_T> createDefaultVertexDataManager(
                    ImmutableClassesGiraphConfiguration conf,
                    IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> frag,
                    long innerVerticesNum) {
        return createDefaultVertexDataManager(
                conf.getVertexValueClass(),
                conf.getGrapeOidClass(),
                conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(),
                conf.getGrapeEdataClass(),
                frag,
                innerVerticesNum,
                conf);
    }

    private static <
                    VDATA_T extends Writable,
                    GRAPE_OID_T,
                    GRAPE_VID_T,
                    GRAPE_VDATA_T,
                    GRAPE_EDATA_T>
            VertexDataManager<VDATA_T> createDefaultVertexDataManager(
                    Class<? extends VDATA_T> vdataClass,
                    Class<? extends GRAPE_OID_T> grapeOidClass,
                    Class<? extends GRAPE_VID_T> grapeVidClass,
                    Class<? extends GRAPE_VDATA_T> grapeVdataClass,
                    Class<? extends GRAPE_EDATA_T> grapeEdataClass,
                    IFragment fragment,
                    long vertexNum,
                    ImmutableClassesGiraphConfiguration conf) {
        if (conf.getGrapeVdataClass().equals(Double.class)
                && conf.getGrapeVidClass().equals(Long.class)) {
            logger.info("Creating specialized long vid double vertex data manager");
            return new LongVidDoubleVertexDataManagerImpl<
                    VDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                    fragment, vertexNum, conf);
        }
        return new VertexDataManagerImpl<
                VDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, vertexNum, conf);
    }

    public static <
                    OID_T extends WritableComparable,
                    GRAPE_OID_T,
                    GRAPE_VID_T,
                    GRAPE_VDATA_T,
                    GRAPE_EDATA_T>
            VertexIdManager<OID_T> createDefaultVertexIdManager(
                    ImmutableClassesGiraphConfiguration conf,
                    IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> frag,
                    long innerVerticesNum) {
        return createDefaultVertexIdManager(
                conf.getVertexIdClass(),
                conf.getGrapeOidClass(),
                conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(),
                conf.getGrapeEdataClass(),
                frag,
                innerVerticesNum,
                conf);
    }

    private static <
                    OID_T extends WritableComparable,
                    GRAPE_OID_T,
                    GRAPE_VID_T,
                    GRAPE_VDATA_T,
                    GRAPE_EDATA_T>
            VertexIdManager<OID_T> createDefaultVertexIdManager(
                    Class<? extends OID_T> oidClass,
                    Class<? extends GRAPE_OID_T> grapeOidClass,
                    Class<? extends GRAPE_VID_T> grapeVidClass,
                    Class<? extends GRAPE_VDATA_T> grapeVdataClass,
                    Class<? extends GRAPE_EDATA_T> grapeEdataClass,
                    IFragment fragment,
                    long vertexNum,
                    ImmutableClassesGiraphConfiguration conf) {
        if (conf.getGrapeVidClass().equals(Long.class)
                && conf.getGrapeOidClass().equals(Long.class)) {
            logger.info("Creating specialized long vid long oid vertex id manager");
            return new LongVidLongVertexIdManagerImpl<
                    OID_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                    fragment, vertexNum, conf);
        }
        return new VertexIdManagerImpl<
                OID_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, vertexNum, conf);
    }

    public static <
                    OID_T extends WritableComparable,
                    EDATA_T extends Writable,
                    GRAPE_OID_T,
                    GRAPE_VID_T,
                    GRAPE_VDATA_T,
                    GRAPE_EDATA_T>
            ImmutableEdgeManagerImpl<
                            OID_T, EDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
                    createImmutableEdgeManager(
                            ImmutableClassesGiraphConfiguration conf,
                            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
                                    fragment,
                            VertexIdManager<OID_T> vertexIdManager) {
        return createImmutableEdgeManagerImpl(
                conf.getVertexIdClass(),
                conf.getEdgeValueClass(),
                conf.getGrapeOidClass(),
                conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(),
                conf.getGrapeEdataClass(),
                fragment,
                vertexIdManager,
                conf);
    }

    private static <
                    OID_T extends WritableComparable,
                    EDATA_T extends Writable,
                    GRAPE_OID_T,
                    GRAPE_VID_T,
                    GRAPE_VDATA_T,
                    GRAPE_EDATA_T>
            ImmutableEdgeManagerImpl<
                            OID_T, EDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
                    createImmutableEdgeManagerImpl(
                            Class<? extends OID_T> oidClass,
                            Class<? extends EDATA_T> edataClass,
                            Class<? extends GRAPE_OID_T> grapeOidClass,
                            Class<? extends GRAPE_VID_T> grapeVidClass,
                            Class<? extends GRAPE_VDATA_T> grapeVdataClass,
                            Class<? extends GRAPE_EDATA_T> grapeEdataClass,
                            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
                                    fragment,
                            VertexIdManager<OID_T> idManager,
                            ImmutableClassesGiraphConfiguration<OID_T, ?, EDATA_T> conf) {
        return new ImmutableEdgeManagerImpl<
                OID_T, EDATA_T, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>(
                fragment, idManager, conf);
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> createGiraphComputationAdaptor(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VID_T> vidClass,
                    Class<? extends VDATA_T> vdataClass,
                    Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T>
            GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>
                    createGiraphComputationAdaptorContext(
                            Class<? extends OID_T> oidClass,
                            Class<? extends VID_T> vidClass,
                            Class<? extends VDATA_T> vdataClass,
                            Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    /**
     * Get the actual argument a child class has to implement a generic interface.
     *
     * @param baseClass baseclass
     * @param childClass child class
     * @param <T> type to evaluation
     * @return
     */
    private static <T> Class<?>[] getTypeArgumentFromInterface(
            Class<T> baseClass, Class<? extends T> childClass) {
        Type type = childClass.getGenericInterfaces()[0];
        Class<?>[] classes;
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] typeParams = parameterizedType.getActualTypeArguments();
            classes = new Class<?>[typeParams.length];
            for (int i = 0; i < typeParams.length; ++i) {
                classes[i] = (Class<?>) typeParams[i];
            }
            return classes;
        } else {
            throw new IllegalStateException("Not a parameterized type");
        }
    }
}
