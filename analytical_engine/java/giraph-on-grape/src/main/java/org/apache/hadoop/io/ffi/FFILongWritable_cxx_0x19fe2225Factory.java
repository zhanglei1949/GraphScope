package org.apache.hadoop.io.ffi;

public class FFILongWritable_cxx_0x19fe2225Factory implements FFILongWritable.Factory {
  public static final FFILongWritable.Factory INSTANCE;

  static {
    INSTANCE = new FFILongWritable_cxx_0x19fe2225Factory();
  }

  public FFILongWritable_cxx_0x19fe2225Factory() {
  }

  public FFILongWritable create() {
    return new FFILongWritable_cxx_0x19fe2225(FFILongWritable_cxx_0x19fe2225.nativeCreateFactory0());
  }
}
