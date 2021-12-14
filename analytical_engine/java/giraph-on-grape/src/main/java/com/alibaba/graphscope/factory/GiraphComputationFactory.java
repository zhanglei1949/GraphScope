package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.app.GiraphComputationAdaptor;
import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.fragment.SimpleFragment;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GiraphComputationFactory {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationFactory.class);

    public static GiraphComputationAdaptor createGiraphComputationAdaptor(String className,
        SimpleFragment fragment) {
        Class<?>[] classes = getTypeArgumentFromInterface(SimpleFragment.class,
            fragment.getClass());
        if (classes.length != 4) {
            logger.error("Expected 4 type params, parsed: " + classes.length);
            return null;
        }
        return createGiraphComputationAdaptor(classes[0], classes[1], classes[2], classes[3]);
    }

    public static GiraphComputationAdaptorContext createGiraphComputationAdaptorContext(String className,
        SimpleFragment fragment) {
        Class<?>[] classes = getTypeArgumentFromInterface(SimpleFragment.class,
            fragment.getClass());
        if (classes.length != 4) {
            logger.error("Expected 4 type params, parsed: " + classes.length);
            return null;
        }
        return createGiraphComputationAdaptorContext(classes[0], classes[1], classes[2], classes[3]);
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T> GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> createGiraphComputationAdaptor(
        Class<? extends OID_T> oidClass, Class<? extends VID_T> vidClass,
        Class<? extends VDATA_T> vdataClass, Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    private static <OID_T, VID_T, VDATA_T, EDATA_T> GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T> createGiraphComputationAdaptorContext(
        Class<? extends OID_T> oidClass, Class<? extends VID_T> vidClass,
        Class<? extends VDATA_T> vdataClass, Class<? extends EDATA_T> edataClass) {
        return new GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T>();
    }

    /**
     * Get the actual argument a child class has to implement a generic interface.
     *
     * @param baseClass  baseclass
     * @param childClass child class
     * @param <T>        type to evaluation
     * @return
     */
    private static <T> Class<?>[] getTypeArgumentFromInterface(Class<T> baseClass,
        Class<? extends T> childClass) {
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
