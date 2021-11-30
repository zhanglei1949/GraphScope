#ifndef _JNI_ORG_APACHE_HADOOP_IO_FFI_FFILONGWRITABLE_CXX_0X19FE2225_H
#define _JNI_ORG_APACHE_HADOOP_IO_FFI_FFILONGWRITABLE_CXX_0X19FE2225_H
#include <utility>
#include <jni.h>
#include <new>
namespace giraph {
struct LongWritable {
	jlong value;

	LongWritable()  : value(0) {}
	LongWritable(const LongWritable &from)  : value(from.value) {}
	LongWritable(LongWritable &&from)  : value(from.value) {}
	giraph::LongWritable & operator = (const giraph::LongWritable & from) {
		if (this == &from) return *this;
		value = from.value;
		return *this;
	}
	giraph::LongWritable & operator = (giraph::LongWritable && from) {
		if (this == &from) return *this;
		value = from.value;
		return *this;
	}
}; // end of type declaration
} // end namespace giraph
#endif // _JNI_ORG_APACHE_HADOOP_IO_FFI_FFILONGWRITABLE_CXX_0X19FE2225_H
