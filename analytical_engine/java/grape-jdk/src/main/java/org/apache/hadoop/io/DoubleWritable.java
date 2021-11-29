package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.ffi.FFIDoubleWritable;

public class DoubleWritable implements WritableComparable {
    private FFIDoubleWritable ffiDoubleWritable;

    public DoubleWritable() {
        ffiDoubleWritable = FFIDoubleWritable.newInstance();
    }

    public DoubleWritable(double value) {
        ffiDoubleWritable = FFIDoubleWritable.newInstance();
        ffiDoubleWritable.value(value);
    }

    public void set(double value) {
        ffiDoubleWritable.value(value);
    }

    public double get() {
        return ffiDoubleWritable.value();
    }

    public void readFields(DataInput in) throws IOException {
        ffiDoubleWritable.value(in.readDouble());
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(ffiDoubleWritable.value());
    }

    public boolean equals(Object o) {
        if (!(o instanceof DoubleWritable)) {
            return false;
        } else {
            DoubleWritable other = (DoubleWritable) o;
            return this.ffiDoubleWritable.value() == other.ffiDoubleWritable.value();
        }
    }

    public int hashCode() {
        return (int) this.ffiDoubleWritable.value();
    }

    @Override
    public int compareTo(Object o) {
        double thisValue = this.ffiDoubleWritable.value();
        double thatValue = ((DoubleWritable) o).ffiDoubleWritable.value();
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Double.toString(this.ffiDoubleWritable.value());
    }

    static {
        WritableComparator.define(DoubleWritable.class, new DoubleWritable.Comparator());
    }

    public static class DecreasingComparator extends DoubleWritable.Comparator {
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
            super(DoubleWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
