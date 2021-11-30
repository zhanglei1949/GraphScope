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
    header = "jni_org_apache_hadoop_io_ffi_FFIDoubleWritable_cxx_0xaafe5c3a.h",
    name = "DoubleWritable",
    namespace = "giraph",
    fields = @FFIMirrorFieldDefinition(name = "value", foreignType = "jdouble", javaType = "double")
)
@FFIForeignType(
    value = "giraph::DoubleWritable",
    factory = FFIDoubleWritable_cxx_0xaafe5c3aFactory.class
)
@FFISynthetic("org.apache.hadoop.io.ffi.FFIDoubleWritable")
public class FFIDoubleWritable_cxx_0xaafe5c3a extends FFIPointerImpl implements FFIDoubleWritable {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("giraph-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(FFIDoubleWritable_cxx_0xaafe5c3a.class, "giraph-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public FFIDoubleWritable_cxx_0xaafe5c3a(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FFIDoubleWritable_cxx_0xaafe5c3a that = (FFIDoubleWritable_cxx_0xaafe5c3a) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFIGetter
  public double value() {
    return nativeValue0(address);
  }

  @FFIGetter
  public static native double nativeValue0(long ptr);

  @FFISetter
  public void value(double val) {
    nativeValue1(address, val);
  }

  @FFISetter
  public static native void nativeValue1(long ptr, double val0);

  public static native long nativeCreateFactory0();
}
