package org.apache.giraph.conf;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import org.apache.giraph.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holding type arguments for grape fragment.
 */
public class GrapeTypes {

    private static Logger logger = LoggerFactory.getLogger(GrapeTypes.class);

    private Class<?> oidClass;
    private Class<?> vidClass;
    private Class<?> vdataClass;
    private Class<?> edataClass;

    public GrapeTypes(
        SimpleFragment fragment) {
        parseFromSimpleFragment(fragment);
    }

    public boolean hasData() {
        return oidClass != null && vidClass != null && vdataClass != null && edataClass != null;
    }

    public boolean parseFromSimpleFragment(SimpleFragment fragment) {

        Class<? extends SimpleFragment> fragmentClass = fragment.getClass();
        Class<?> classes[];
        if (ImmutableEdgecutFragmentAdaptor.class.isAssignableFrom(fragmentClass)) {
            ImmutableEdgecutFragmentAdaptor adaptor = (ImmutableEdgecutFragmentAdaptor) fragment;
            classes = ReflectionUtils
                .getTypeArgumentFromInterface(ImmutableEdgecutFragment.class,
                    adaptor.getImmutableFragment().getClass());
        } else if (ArrowProjectedFragment.class.isAssignableFrom(fragmentClass)) {
            ArrowProjectedAdaptor adaptor = (ArrowProjectedAdaptor) fragment;
            classes = ReflectionUtils
                .getTypeArgumentFromInterface(ArrowProjectedFragment.class,
                    adaptor.getArrowProjectedFragment().getClass());
        } else {
            return false;
        }

        if (classes.length != 4) {
            throw new IllegalStateException(
                "Expected 4 actual types, but received class array of length: " + classes.length);
        }
        oidClass = classes[0];
        vidClass = classes[1];
        vdataClass = classes[2];
        edataClass = classes[3];
        logger.info(
            "Grape types: oid: " + oidClass.getName() + " vid: " + vidClass.getName() + " vdata: "
                + vdataClass.getName() + " edata: " + edataClass.getName());
        return true;
    }

    public Class<?> getOidClass() {
        return oidClass;
    }

    public Class<?> getVidClass() {
        return vidClass;
    }

    public Class<?> getVdataClass() {
        return vdataClass;
    }

    public Class<?> getEdataClass() {
        return getEdataClass();
    }
}
