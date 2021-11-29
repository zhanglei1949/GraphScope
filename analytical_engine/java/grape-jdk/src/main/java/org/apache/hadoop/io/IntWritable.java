package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.ffi.FFIIntWritable;

public class IntWritable implements WritableComparable {
    private FFIIntWritable ffiIntWritable;

    public IntWritable() {
        ffiIntWritable = FFIIntWritable.newInstance();
    }

    public IntWritable(int value) {
        ffiIntWritable = FFIIntWritable.newInstance();
        ffiIntWritable.value(value);
    }

    public void set(int value) {
        ffiIntWritable.value(value);
    }

    public long get() {
        return ffiIntWritable.value();
    }

    public void readFields(DataInput in) throws IOException {
        ffiIntWritable.value(in.readInt());
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(ffiIntWritable.value());
    }

    public boolean equals(Object o) {
        if (!(o instanceof IntWritable)) {
            return false;
        } else {
            IntWritable other = (IntWritable) o;
            return this.ffiIntWritable.value() == other.ffiIntWritable.value();
        }
    }

    public int hashCode() {
        return this.ffiIntWritable.value();
    }

    @Override
    public int compareTo(Object o) {
        int thisValue = this.ffiIntWritable.value();
        int thatValue = ((IntWritable) o).ffiIntWritable.value();
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Integer.toString(this.ffiIntWritable.value());
    }

    static {
        WritableComparator.define(IntWritable.class, new IntWritable.Comparator());
    }

    public static class DecreasingComparator extends IntWritable.Comparator {
        public DecreasingComparator() {}

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
