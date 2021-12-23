package org.apache.giraph.comm.requests;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NettyWritableMessage implements NettyMessage {

    private Writable data;
    private int repeatTimes;

    public NettyWritableMessage(Writable data, int repeatTimes) {
        this.data = data;
        this.repeatTimes = repeatTimes;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.repeatTimes = input.readInt();
        for (int i = 0; i < repeatTimes; ++i) {
            this.data.readFields(input);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(repeatTimes);
        for (int i = 0; i < repeatTimes; ++i) {
            this.data.write(output);
        }
    }

    @Override
    public int getSerializedSize() {
        // Caution.
        return 4 + repeatTimes * 8;
    }

    @Override
    public NettyMessageType getMessageType() {
        return NettyMessageType.NETTY_WRITABLE_MESSAGE;
    }
}
