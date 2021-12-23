package org.apache.giraph.comm.requests;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NettyWritableMessage implements NettyMessage {

    private Writable data;
    private int repeatTimes;
    private int writableType;

    public NettyWritableMessage() {}

    public NettyWritableMessage(Writable data, int repeatTimes) {
        this.data = data;
        this.repeatTimes = repeatTimes;
        if (data instanceof LongWritable) {
            writableType = 1;
        } else {
            writableType = 0;
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.repeatTimes = input.readInt();
        this.writableType = input.readInt();
        if (this.writableType == 1) {
            data = new LongWritable();
        } else {
            data = new IntWritable();
        }
        for (int i = 0; i < repeatTimes; ++i) {
            this.data.readFields(input);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(repeatTimes);
        output.writeInt(writableType);
        for (int i = 0; i < repeatTimes; ++i) {
            this.data.write(output);
        }
    }

    @Override
    public int getSerializedSize() {
        // Caution.
        return 4 + 4 + repeatTimes * 8;
    }

    @Override
    public NettyMessageType getMessageType() {
        return NettyMessageType.NETTY_WRITABLE_MESSAGE;
    }
}
