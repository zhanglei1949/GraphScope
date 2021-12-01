package org.apache.hadoop.io;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.stdcxx.StdVector;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.Assert;
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
	static {
		System.loadLibrary("giraph-jni");
	}
    private static final int VECTOR_LENGTH = 1024 * 1024 * 10;

    private static final int ALLOCATOR_CAPACITY = 1024 * 1024 *10 * 8;
    private StdVector.Factory vectorFactory = FFITypeFactory.getFactory(StdVector.class, "std::vector<int64_t>");
    private StdVector<Long> vector = vectorFactory.create();


    private BufferAllocator allocator;

    private IntVector arrowVector;

    @Setup
    public void prepare() {
        vector.reserve(VECTOR_LENGTH);
        for (long i = 0; i < VECTOR_LENGTH; i++) {
            vector.set(i, i);
        }

        allocator = new RootAllocator(ALLOCATOR_CAPACITY );
        arrowVector = new IntVector("vector", allocator);
        arrowVector.allocateNew(VECTOR_LENGTH);
        arrowVector.setValueCount(VECTOR_LENGTH);

        for (int i = 0; i < VECTOR_LENGTH; i++) {
            arrowVector.set(i, i);
        }
    }

    @TearDown
    public void tearDown() {
        //In tear down, we check the equality
        vector.setAddress(arrowVector.getDataBufferAddress());
        //for (int i = 0; i < arrowVector.getValueCount(); ++i){
        //   Assert.assertTrue(vector.get(i) == i);
       // }
        allocator.close();
        arrowVector.close();
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
