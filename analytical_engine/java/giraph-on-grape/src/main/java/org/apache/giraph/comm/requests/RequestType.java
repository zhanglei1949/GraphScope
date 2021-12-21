package org.apache.giraph.comm.requests;

public enum RequestType {
    SIMPLE_WRITABLE(SimpleWritable.class);

    private Class<? extends WritableRequest> requestClass;
private    RequestType(Class<? extends WritableRequest> clz){
    this.requestClass = clz;
}

    public Class<? extends WritableRequest> getRequestClass() {
        return requestClass;
    }
}
