package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.ffi.FFILongWritable;

public class LongWritable implements WritableComparable {
    private FFILongWritable ffiLongWritable;

    public LongWritable() {
        ffiLongWritable = FFILongWritable.newInstance();
    }

    public LongWritable(long value) {
        ffiLongWritable = FFILongWritable.newInstance();
        ffiLongWritable.value(value);
    }

    public void set(long value) {
        ffiLongWritable.value(value);
    }

    public long get() {
        return ffiLongWritable.value();
    }

    public void readFields(DataInput in) throws IOException {
        ffiLongWritable.value(in.readLong());
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(ffiLongWritable.value());
    }

    public boolean equals(Object o) {
        if (!(o instanceof LongWritable)) {
            return false;
        } else {
            LongWritable other = (LongWritable) o;
            return this.ffiLongWritable.value() == other.ffiLongWritable.value();
        }
    }

    public int hashCode() {
        return (int) this.ffiLongWritable.value();
    }

    @Override
    public int compareTo(Object o) {
        long thisValue = this.ffiLongWritable.value();
        long thatValue = ((LongWritable) o).ffiLongWritable.value();
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Long.toString(this.ffiLongWritable.value());
    }

    static {
        WritableComparator.define(LongWritable.class, new LongWritable.Comparator());
    }

    public static class DecreasingComparator extends LongWritable.Comparator {
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
            super(LongWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            long thisValue = readLong(b1, s1);
            long thatValue = readLong(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
