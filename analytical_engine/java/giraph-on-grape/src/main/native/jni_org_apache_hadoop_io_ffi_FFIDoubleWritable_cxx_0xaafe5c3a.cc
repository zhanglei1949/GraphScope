#include "giraph-jni-ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

namespace giraph {
// Common Stubs

JNIEXPORT
jint JNICALL Java_org_apache_hadoop_io_ffi_FFIDoubleWritable_1cxx_10xaafe5c3a__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(giraph::DoubleWritable);
}

JNIEXPORT
jdouble JNICALL Java_org_apache_hadoop_io_ffi_FFIDoubleWritable_1cxx_10xaafe5c3a_nativeValue0(JNIEnv*, jclass, jlong ptr) {
	return (jdouble)(reinterpret_cast<giraph::DoubleWritable*>(ptr)->value);
}

JNIEXPORT
void JNICALL Java_org_apache_hadoop_io_ffi_FFIDoubleWritable_1cxx_10xaafe5c3a_nativeValue1(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* val0 */) {
	reinterpret_cast<giraph::DoubleWritable*>(ptr)->value = arg0;
}

JNIEXPORT
jlong JNICALL Java_org_apache_hadoop_io_ffi_FFIDoubleWritable_1cxx_10xaafe5c3a_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new giraph::DoubleWritable());
}

} // end namespace giraph
#ifdef __cplusplus
}
#endif
