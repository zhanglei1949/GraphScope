package org.apache.hadoop.io;

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

    private LongWritable[] longWritables;
    private int SIZE = 1024 * 1024;

    @Setup
    public void prepare(){
        longWritables = new LongWritable[SIZE];
        for (int i = 0; i < SIZE; ++i){
            longWritables[i] = new LongWritable(i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public int read() {
        int result = 0;
        for (int i = 0; i < SIZE; ++i) {
            result += longWritables[i].get();
        }
        return result;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void write() {
        for (int i = 0; i < SIZE; ++i) {
            longWritables[i].set(i);
        }
    }
}
