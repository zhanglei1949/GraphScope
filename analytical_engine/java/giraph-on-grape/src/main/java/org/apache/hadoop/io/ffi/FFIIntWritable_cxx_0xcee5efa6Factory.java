package org.apache.hadoop.io.ffi;

public class FFIIntWritable_cxx_0xcee5efa6Factory implements FFIIntWritable.Factory {
  public static final FFIIntWritable.Factory INSTANCE;

  static {
    INSTANCE = new FFIIntWritable_cxx_0xcee5efa6Factory();
  }

  public FFIIntWritable_cxx_0xcee5efa6Factory() {
  }

  public FFIIntWritable create() {
    return new FFIIntWritable_cxx_0xcee5efa6(FFIIntWritable_cxx_0xcee5efa6.nativeCreateFactory0());
  }
}
