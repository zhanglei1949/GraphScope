package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import org.apache.tools.ant.taskdefs.Java;

/**
 * This is a FFIWrapper for std::vector. The code origins from the generated code via FFI and llvm4jni,
 * with hands-on optimization.
 */
@FFIForeignType(
    value = "std::vector<char>",
    factory = FFIByteVectorFactory.class
)
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdVector")
public class FFIByteVector extends FFIPointerImpl implements StdVector<Byte>{
    public static final int SIZE = _elementSize$$$();
    private long objAddress;
    public static final int HASH_SHIFT;
    public long capacity;

    public FFIByteVector(long address) {
        super(address);
        objAddress = JavaRuntime.getLong(address);
        capacity = nativeCapacity(address);
    }

    private static final int _elementSize$$$() {
        return 24;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            FFIByteVector that = (FFIByteVector) o;
            return this.address == that.address;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return (int)(this.address >> HASH_SHIFT);
    }

    public String toString() {
        return this.getClass().getName() + "@" + Long.toHexString(this.address);
    }

    public long capacity() {
        return nativeCapacity(this.address);
    }

    public static long nativeCapacity(long var0) {
        long var2 = JavaRuntime.getLong(var0 + 16L);
        long var4 = JavaRuntime.getLong(var0);
        return var2 - var4;
    }

    public void clear() {
        nativeClear(this.address);
    }

    public static void nativeClear(long var0) {
        long var2 = JavaRuntime.getLong(var0);
        long var4 = var0 + 8L;
        if (JavaRuntime.getLong(var4) != var2) {
            JavaRuntime.putLong(var4, var2);
        }

    }

    public long data() {
        return nativeData(this.address);
    }

    public static long nativeData(long var0) {
        return JavaRuntime.getLong(var0);
    }

    @CXXOperator("delete")
    public void delete() {
        nativeDelete(this.address);
    }

    @CXXOperator("delete")
    public static native void nativeDelete(long var0);

    @CXXOperator("[]")
    @CXXReference
    public Byte get(long arg0) {
//        return nativeGet(this.address, arg0);
        return (byte) JavaRuntime.getByte(objAddress + arg0);
    }

    public byte getRaw(long arg0) {
//        return nativeGet(this.address, arg0);
        return JavaRuntime.getByte(objAddress + arg0);
    }
    public char getRawChar(long arg0) {
//        return nativeGet(this.address, arg0);
        return JavaRuntime.getChar(objAddress + arg0);
    }
    public int getRawInt(long arg0){
        return JavaRuntime.getInt(objAddress + arg0);
    }

    public long getRawLong(long arg0){
        return JavaRuntime.getLong(objAddress + arg0);
    }

    @CXXOperator("[]")
    @CXXReference
    public static byte nativeGet(long var0, long var2) {
        return (byte)JavaRuntime.getByte(JavaRuntime.getLong(var0) + var2);
    }

    public void push_back(@CXXValue Byte arg0) {
        nativePush_back(this.address, arg0);
    }

    public static native void nativePush_back(long var0, byte var2);

    public void reserve(long arg0) {
        nativeReserve(this.address, arg0);
    }

    public static native void nativeReserve(long var0, long var2);

    public void resize(long arg0) {
        nativeResize(this.address, arg0);
    }

    public static native void nativeResize(long var0, long var2);

    public void ensure(long offset, int size){
        long minCapacity = size + offset;
        long oldCapacity = capacity;
        if (minCapacity <= oldCapacity) {
            return;
        }
        long newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0){
            newCapacity = minCapacity;
        }
//        this.base = reserve(newCapacity);
        nativeReserve(address, newCapacity);
        this.objAddress = JavaRuntime.getLong(address);
        this.capacity = newCapacity;
    }

    @CXXOperator("[]")
    public void set(long arg0, @CXXReference Byte arg1) {
        ensure(arg0, 1);
//        nativeSet(this.address, arg0, arg1);
        JavaRuntime.putByte(objAddress + arg0, arg1);
    }

    public void setRawByte(long arg0, byte arg1) {
//        ensure(arg0, 1);
        JavaRuntime.putByte(objAddress + arg0, arg1);
    }

    public void setRawShort(long arg0, short arg1) {
//        ensure(arg0, 2);
        JavaRuntime.putShort(objAddress + arg0, arg1);
    }
    public void setRawChar(long arg0, char arg1) {
//        ensure(arg0, 2);
        JavaRuntime.putChar(objAddress + arg0, arg1);
    }

    public void setRawInt(long arg0, int arg1) {
//        ensure(arg0, 4);
        JavaRuntime.putInt(objAddress + arg0, arg1);
    }
    public void setRawLong(long arg0, long arg1) {
//        ensure(arg0, 8);
        JavaRuntime.putLong(objAddress + arg0, arg1);
    }
    public void setRawFloat(long arg0, float arg1) {
//        ensure(arg0, 4);
        JavaRuntime.putFloat(objAddress + arg0, arg1);
    }

    public void setRawDouble(long arg0, double arg1) {
//        ensure(arg0, 8);
        JavaRuntime.putDouble(objAddress + arg0, arg1);
    }

    @CXXOperator("[]")
    public static void nativeSet(long var0, long var2, byte var4) {
        JavaRuntime.putByte(JavaRuntime.getLong(var0) + var2, var4);
    }

    public void setAddress(long arg0) {
        this.address = arg0;
    }

    public long size() {
        return nativeSize(this.address);
    }

    public static long nativeSize(long var0) {
        long var2 = JavaRuntime.getLong(var0 + 8L);
        long var4 = JavaRuntime.getLong(var0);
        return var2 - var4;
    }

    public static native long nativeCreateFactory0();

    static {
        assert SIZE > 0;

        HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);

        assert HASH_SHIFT > 0;

    }
}

