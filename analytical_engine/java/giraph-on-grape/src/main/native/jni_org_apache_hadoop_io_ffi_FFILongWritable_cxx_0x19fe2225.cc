#include "giraph-jni-ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

namespace giraph {
// Common Stubs

JNIEXPORT
jint JNICALL Java_org_apache_hadoop_io_ffi_FFILongWritable_1cxx_10x19fe2225__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(giraph::LongWritable);
}

JNIEXPORT
jlong JNICALL Java_org_apache_hadoop_io_ffi_FFILongWritable_1cxx_10x19fe2225_nativeValue0(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<giraph::LongWritable*>(ptr)->value);
}

JNIEXPORT
void JNICALL Java_org_apache_hadoop_io_ffi_FFILongWritable_1cxx_10x19fe2225_nativeValue1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* val0 */) {
	reinterpret_cast<giraph::LongWritable*>(ptr)->value = arg0;
}

JNIEXPORT
jlong JNICALL Java_org_apache_hadoop_io_ffi_FFILongWritable_1cxx_10x19fe2225_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new giraph::LongWritable());
}

} // end namespace giraph
#ifdef __cplusplus
}
#endif
