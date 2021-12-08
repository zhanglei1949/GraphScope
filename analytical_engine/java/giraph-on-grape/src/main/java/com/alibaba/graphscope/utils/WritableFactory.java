package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.parallel.impl.GiraphDefaultMessageManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableFactory {

    private static Logger logger = LoggerFactory.getLogger(WritableFactory.class);
    private static Class<? extends Writable> inMsgClass;
    private static Class<? extends Writable> outMsgClass;

    private static Class<? extends WritableComparable> oidClass;

    public static void setInMsgClass(Class<? extends Writable> param){
        inMsgClass = param;
    }
    public static void setOutMsgClass(Class<? extends Writable> param){
        outMsgClass = param;
    }
    public static void setOidClass(Class<? extends WritableComparable> param){
        oidClass = param;
    }

    public static WritableComparable newOid(){
        if (oidClass == null){
            logger.error("Set oid class first");
        }
        try {
            return oidClass.newInstance();
        } catch (InstantiationException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    public static Writable newInMsg(){
        if (inMsgClass == null){
            logger.error("Set oid class first");
        }
        try {
            return inMsgClass.newInstance();
        } catch (InstantiationException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    public static Writable newOutMsg(){
        if (outMsgClass == null){
            logger.error("Set oid class first");
        }
        try {
            return outMsgClass.newInstance();
        } catch (InstantiationException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

}
