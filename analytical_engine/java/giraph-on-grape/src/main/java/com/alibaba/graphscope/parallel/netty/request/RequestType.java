package com.alibaba.graphscope.parallel.netty.request;

import com.alibaba.graphscope.parallel.netty.request.impl.OneLongWritableRequest;

public enum RequestType {
    ONE_LONG_WRITABLE_REQUEST(OneLongWritableRequest.class);


    private Class<? extends WritableRequest> clz;

    RequestType(Class<? extends WritableRequest> clz){
        this.clz = clz;
    }

    public Class<? extends WritableRequest> getClazz(){
        return clz;
    }

}
