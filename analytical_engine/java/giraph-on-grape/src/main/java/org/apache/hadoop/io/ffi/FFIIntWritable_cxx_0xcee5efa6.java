package org.apache.hadoop.io.ffi;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIMirrorDefinition;
import com.alibaba.fastffi.FFIMirrorFieldDefinition;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISetter;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIMirrorDefinition(
    header = "jni_org_apache_hadoop_io_ffi_FFIIntWritable_cxx_0xcee5efa6.h",
    name = "IntWritable",
    namespace = "giraph",
    fields = @FFIMirrorFieldDefinition(name = "value", foreignType = "jint", javaType = "int")
)
@FFIForeignType(
    value = "giraph::IntWritable",
    factory = FFIIntWritable_cxx_0xcee5efa6Factory.class
)
@FFISynthetic("org.apache.hadoop.io.ffi.FFIIntWritable")
public class FFIIntWritable_cxx_0xcee5efa6 extends FFIPointerImpl implements FFIIntWritable {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("giraph-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(FFIIntWritable_cxx_0xcee5efa6.class, "giraph-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public FFIIntWritable_cxx_0xcee5efa6(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FFIIntWritable_cxx_0xcee5efa6 that = (FFIIntWritable_cxx_0xcee5efa6) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFIGetter
  public int value() {
    return nativeValue0(address);
  }

  @FFIGetter
  public static native int nativeValue0(long ptr);

  @FFISetter
  public void value(int val) {
    nativeValue1(address, val);
  }

  @FFISetter
  public static native void nativeValue1(long ptr, int val0);

  public static native long nativeCreateFactory0();
}
