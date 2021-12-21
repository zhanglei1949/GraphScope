package org.apache.giraph.comm.requests;

public enum RequestType {
    SIMPLE_WRITABLE(SimpleLongWritableRequest.class);

    private Class<? extends WritableRequest> requestClass;
private    RequestType(Class<? extends WritableRequest> clz){
    this.requestClass = clz;
}

    public Class<? extends WritableRequest> getRequestClass() {
        return requestClass;
    }
}
