package org.apache.giraph.conf;

import com.alibaba.graphscope.fragment.SimpleFragment;
import org.apache.giraph.utils.ReflectionUtils;
import sun.java2d.pipe.SpanShapeRenderer.Simple;

/**
 * Holding type arguments for grape fragment.
 */
public class GrapeTypes {
    private Class<?> oidClass;
    private Class<?> vidClass;
    private Class<?> vdataClass;
    private Class<?> edataClass;

    public GrapeTypes(
        Class<? extends SimpleFragment> fragmentClass){
        parseFromSimpleFragment(fragmentClass);
    }

    public boolean hasData(){
        return oidClass != null && vidClass != null && vdataClass != null && edataClass != null;
    }

    public boolean parseFromSimpleFragment(Class<? extends SimpleFragment> fragmentClass){
        Class<?> classes[] = ReflectionUtils
            .getTypeArgumentFromInterface(SimpleFragment.class, fragmentClass);
        if (classes.length != 4){
            throw new IllegalStateException("Expected 4 actual types, but received class array of length: " + classes.length);
        }
        oidClass = classes[0];
        vidClass = classes[1];
        vdataClass = classes[2];
        edataClass = classes[3];
        return true;
    }

    public Class<?> getOidClass(){
        return oidClass;
    }
    public Class<?> getVidClass(){
        return vidClass;
    }
    public Class<?> getVdataClass(){
        return vdataClass;
    }
    public Class<?> getEdataClass(){
        return getEdataClass();
    }
}
