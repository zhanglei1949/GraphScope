package org.apache.hadoop.io;

import com.alibaba.fastffi.CXXValueScope;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class LongWritableTest {
    static {
        System.loadLibrary("giraph-jni");
    }

    private LongWritable[] longWritables;
    private LongWritable[] readWritable;
    private int SIZE = 1024 * 1024;

    @Setup
    public void prepare() {
        readWritable = new LongWritable[SIZE];
        for (int i = 0; i < SIZE; ++i) {
            readWritable[i] = new LongWritable(i);
        }
    }

    @TearDown
    public void cleanUp() {}

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void allocatingFFI() {
        try (CXXValueScope scope = new CXXValueScope()) {
            longWritables = new LongWritable[SIZE];
            for (int i = 0; i < SIZE; ++i) {
                longWritables[i] = new LongWritable(i);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void read() {
        int result = 0;
        for (int i = 0; i < SIZE; ++i) {
            result += readWritable[i].get();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void write() {
        int result = 0;
        for (int i = 0; i < SIZE; ++i) {
            readWritable[i].set(i);
        }
    }
}
