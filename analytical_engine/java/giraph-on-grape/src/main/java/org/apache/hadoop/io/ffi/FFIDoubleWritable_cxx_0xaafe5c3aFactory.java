package org.apache.hadoop.io.ffi;

public class FFIDoubleWritable_cxx_0xaafe5c3aFactory implements FFIDoubleWritable.Factory {
  public static final FFIDoubleWritable.Factory INSTANCE;

  static {
    INSTANCE = new FFIDoubleWritable_cxx_0xaafe5c3aFactory();
  }

  public FFIDoubleWritable_cxx_0xaafe5c3aFactory() {
  }

  public FFIDoubleWritable create() {
    return new FFIDoubleWritable_cxx_0xaafe5c3a(FFIDoubleWritable_cxx_0xaafe5c3a.nativeCreateFactory0());
  }
}
