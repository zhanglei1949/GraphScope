package org.apache.giraph.comm.requests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SimpleLongWritableRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> extends WritableRequest<I,V,E>{


    public LongWritable writable;

    public SimpleLongWritableRequest(){
        writable = new LongWritable();
    }

    public SimpleLongWritableRequest(LongWritable value){
        writable = value;
    }

    /**
     * Get the type of the request
     *
     * @return Request type
     */
    @Override
    public RequestType getType() {
        return RequestType.SIMPLE_WRITABLE;
    }

    /**
     * Serialize the request
     *
     * @param input Input to read fields from
     */
    @Override
    void readFieldsRequest(DataInput input) throws IOException {
        writable.readFields(input);
    }

    /**
     * Deserialize the request
     *
     * @param output Output to write the request to
     */
    @Override
    void writeRequest(DataOutput output) throws IOException {
        writable.write(output);
    }

    @Override
    public int getSerializedSize(){
        return 8;
    }
}
