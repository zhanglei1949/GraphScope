package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.hadoop.io.ffi.FFILongWritable;
import sun.misc.Unsafe;

public class LongWritableImpl {
    private static Unsafe unsafe;
    private static int SIZE  = 1024 * 1024;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe =  (Unsafe) field.get(null);
            System.out.println("Got unsafe");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private long address;

    public LongWritableImpl() {
        address = unsafe.allocateMemory(8 * SIZE);
    }

    public LongWritableImpl(long value) {
        address = unsafe.allocateMemory(8 * SIZE);

    }

    public void set(long value) {
        long cur = address;
        long targetAddress = address + 8 * SIZE;
        long index = 1;
        while (cur < targetAddress){
            unsafe.putLong(cur, index++);
            cur += 8;
        }
    }

    public long get() {
        long result = 0;
        long cur = address;
        long targetAddress = address + 8 * SIZE;
        while (cur < targetAddress){
            result += unsafe.getLong(cur);
            cur += 8;
        }
        return result;
    }
}
