#ifndef _JNI_ORG_APACHE_HADOOP_IO_FFI_FFIINTWRITABLE_CXX_0XCEE5EFA6_H
#define _JNI_ORG_APACHE_HADOOP_IO_FFI_FFIINTWRITABLE_CXX_0XCEE5EFA6_H
#include <utility>
#include <jni.h>
#include <new>
namespace giraph {
struct IntWritable {
	jint value;

	IntWritable()  : value(0) {}
	IntWritable(const IntWritable &from)  : value(from.value) {}
	IntWritable(IntWritable &&from)  : value(from.value) {}
	giraph::IntWritable & operator = (const giraph::IntWritable & from) {
		if (this == &from) return *this;
		value = from.value;
		return *this;
	}
	giraph::IntWritable & operator = (giraph::IntWritable && from) {
		if (this == &from) return *this;
		value = from.value;
		return *this;
	}
}; // end of type declaration
} // end namespace giraph
#endif // _JNI_ORG_APACHE_HADOOP_IO_FFI_FFIINTWRITABLE_CXX_0XCEE5EFA6_H
