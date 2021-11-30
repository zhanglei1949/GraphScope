package org.apache.hadoop.io;

import org.apache.hadoop.io.ffi.FFILongWritable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class LongWritableTest {
    static {
        System.loadLibrary("giraph-jni");
    }

    private FFILongWritable[] ffiLongWritables;
    private LongWritableImpl[] longWritableimpl;
    private int SIZE = 102400;

    @Setup
    public void prepare() {
        ffiLongWritables = new FFILongWritable[SIZE];
        longWritableimpl = new LongWritableImpl[SIZE];
        for (int i = 0; i < SIZE; ++i) {
            ffiLongWritables[i] = FFILongWritable.factory.create();
            ffiLongWritables[i].value(i);
            longWritableimpl[i] = new LongWritableImpl(i);
        }
    }

    @TearDown
    public void cleanUp() {}

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public int readFFI() {
        int result = 0;
        for (int i = 0; i < SIZE; ++i) {
            result += ffiLongWritables[i].value();
        }
        return result;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long unsafeRead() {
        long result = 0;
        for (int i = 0; i < SIZE; ++i) {
            result += longWritableimpl[i].get();
        }
        return result;
    }


    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writeFFI() {
        for (int i = 0; i < SIZE; ++i) {
            ffiLongWritables[i].value(i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void unsafeWrite() {
        for (int i = 0; i < SIZE; ++i) {
            longWritableimpl[i].set(i);
        }
    }
}
