#include "giraph-jni-ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

namespace giraph {
// Common Stubs

JNIEXPORT
jint JNICALL Java_org_apache_hadoop_io_ffi_FFIIntWritable_1cxx_10xcee5efa6__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(giraph::IntWritable);
}

JNIEXPORT
jint JNICALL Java_org_apache_hadoop_io_ffi_FFIIntWritable_1cxx_10xcee5efa6_nativeValue0(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<giraph::IntWritable*>(ptr)->value);
}

JNIEXPORT
void JNICALL Java_org_apache_hadoop_io_ffi_FFIIntWritable_1cxx_10xcee5efa6_nativeValue1(JNIEnv*, jclass, jlong ptr, jint arg0 /* val0 */) {
	reinterpret_cast<giraph::IntWritable*>(ptr)->value = arg0;
}

JNIEXPORT
jlong JNICALL Java_org_apache_hadoop_io_ffi_FFIIntWritable_1cxx_10xcee5efa6_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new giraph::IntWritable());
}

} // end namespace giraph
#ifdef __cplusplus
}
#endif
