package org.apache.hadoop.io.ffi;

import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIMirror;
import com.alibaba.fastffi.FFINameSpace;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFISetter;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

@FFIMirror
@FFIGen(library = "giraph-jni")
@FFINameSpace("giraph")
@FFITypeAlias("DoubleWritable")
public interface FFIDoubleWritable extends FFIPointer {
    Factory factory =
            FFITypeFactory.getFactory(FFIDoubleWritable.Factory.class, FFIDoubleWritable.class);

    static FFIDoubleWritable newInstance() {
        return factory.create();
    }

    @FFISetter
    void value(double val);

    @FFIGetter
    double value();

    @FFIFactory
    interface Factory {
        FFIDoubleWritable create();
    }
}
