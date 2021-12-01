package org.apache.hadoop.io;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.stdcxx.StdVector;
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
public class StdVectorTest {
    private static final int VECTOR_LENGTH = 1024 * 1024 * 10;

    private static final int ALLOCATOR_CAPACITY = 1024 * 1024 *10 * 8;
    private StdVector<Long> vector = FFITypeFactory.getFactory(StdVector.Factory.class, "std::vector<int64_t>");

    @Setup
    public void prepare() {

        for (long i = 0; i < VECTOR_LENGTH; i++) {
            vector.set(i, i);
        }
    }

    @TearDown
    public void tearDown() {
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public int read() {
        int sum = 0;
        for (int i = 0; i < VECTOR_LENGTH; ++i) {
            sum += vector.get(i);
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void write() {
        for (long i = 0; i < VECTOR_LENGTH; ++i){
            vector.set(i, i);
        }
    }

}
