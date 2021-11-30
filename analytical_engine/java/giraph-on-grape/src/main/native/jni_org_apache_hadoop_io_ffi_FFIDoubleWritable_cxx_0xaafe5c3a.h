#ifndef _JNI_ORG_APACHE_HADOOP_IO_FFI_FFIDOUBLEWRITABLE_CXX_0XAAFE5C3A_H
#define _JNI_ORG_APACHE_HADOOP_IO_FFI_FFIDOUBLEWRITABLE_CXX_0XAAFE5C3A_H
#include <utility>
#include <jni.h>
#include <new>
namespace giraph {
struct DoubleWritable {
	jdouble value;

	DoubleWritable()  : value(0.0) {}
	DoubleWritable(const DoubleWritable &from)  : value(from.value) {}
	DoubleWritable(DoubleWritable &&from)  : value(from.value) {}
	giraph::DoubleWritable & operator = (const giraph::DoubleWritable & from) {
		if (this == &from) return *this;
		value = from.value;
		return *this;
	}
	giraph::DoubleWritable & operator = (giraph::DoubleWritable && from) {
		if (this == &from) return *this;
		value = from.value;
		return *this;
	}
}; // end of type declaration
} // end namespace giraph
#endif // _JNI_ORG_APACHE_HADOOP_IO_FFI_FFIDOUBLEWRITABLE_CXX_0XAAFE5C3A_H
