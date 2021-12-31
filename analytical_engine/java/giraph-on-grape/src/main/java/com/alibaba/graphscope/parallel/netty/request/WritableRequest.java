package com.alibaba.graphscope.parallel.netty.request;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public abstract class WritableRequest implements Writable {
    private int clientId;

    public int getClientId(){
        return clientId;
    }

    public void setClientId(int clientId){
        this.clientId = clientId;
    }

    public abstract RequestType getRequestType();

    public abstract void readFieldsRequest(DataInput input) throws  IOException;

    public abstract void writeFieldsRequest(DataOutput output) throws IOException;

    @Override
    public final void readFields(DataInput input) throws IOException {
        clientId = input.readInt();
        readFieldsRequest(input);
    }

    @Override
    public final void write(DataOutput output) throws IOException {
        output.writeInt(clientId);
        writeFieldsRequest(output);
    }

}
