package org.apache.hadoop.io;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.stdcxx.LongStdVector;
import com.alibaba.graphscope.stdcxx.StdVector;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.junit.Assert;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.CompilerControl;
import sun.misc.Unsafe;
import java.lang.reflect.Field;

@State(Scope.Benchmark)
public class StdVectorTest {
	static {
		System.loadLibrary("giraph-jni");
	}
    private static Unsafe unsafe;
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
    private static final int VECTOR_LENGTH = 1024 * 1024 * 8;

    private static final int ALLOCATOR_CAPACITY = 1024 * 1024 * 8 * 64;

    private LongStdVector.Factory vectorFactory = FFITypeFactory.getFactory(LongStdVector.class, "std::vector<int64_t>");
    private LongStdVector vector = vectorFactory.create();
    private long vectorAddr = vector.getAddress();

    private long stdVectorFirstAddr;

    private BufferAllocator allocator;

    private ArrowBuf arrowBuf;

    @Setup
    public void prepare() {
        vector.resize(VECTOR_LENGTH);
        for (long i = 0; i < VECTOR_LENGTH; i++) {
            vector.set(i, i);
        }

        allocator = new RootAllocator(ALLOCATOR_CAPACITY );
        arrowBuf = allocator.buffer(VECTOR_LENGTH * Long.BYTES);
        for (long i = 0; i < VECTOR_LENGTH; ++i){
            arrowBuf.setLong(i * 8, i);
        }
	stdVectorFirstAddr = unsafe.getLong(vectorAddr);
	System.out.println("stdvector addr: " + vectorAddr + ", " + stdVectorFirstAddr);
    }

    @TearDown
    public void tearDown() {
        //In tear down, we check the equality
//        vector.setAddress(arrowVector.getDataBufferAddress());
        for (int i = 0; i < VECTOR_LENGTH; ++i){
//           Assert.assertTrue(vector.get(i) == i);
//            Assert.assertTrue(arrowBuf.getLong(i) == i);
		if (arrowBuf.getLong(i * 8) != i){
			System.out.println("value not match: " + i + "," + arrowBuf.getLong(i * 8));
			break;
		}
        }
        arrowBuf.close();
        allocator.close();

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
    public int readArrow() {
        int sum = 0;
        for (int i = 0; i < VECTOR_LENGTH; ++i) {
            sum += arrowBuf.getLong(i * 8);
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @CompilerControl(CompilerControl.Mode.PRINT)
    public void write() {
        for (long i = 0; i < VECTOR_LENGTH; ++i){
            vector.set(i, i);
        }
    }
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @CompilerControl(CompilerControl.Mode.PRINT)
    public void writeDirectly() {
        for (long i = 0; i < VECTOR_LENGTH; ++i){
	    unsafe.putLong( stdVectorFirstAddr + i * 8, i);
        }
    }
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @CompilerControl(CompilerControl.Mode.PRINT)
    public void writeArrow() {
        for (long i = 0; i < VECTOR_LENGTH; ++i){
            arrowBuf.setLong(i * 8, i);
        }
    }
}
