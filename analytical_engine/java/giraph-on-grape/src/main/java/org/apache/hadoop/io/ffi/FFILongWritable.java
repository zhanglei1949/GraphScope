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
@FFITypeAlias("LongWritable")
public interface FFILongWritable extends FFIPointer {
    Factory factory =
            FFITypeFactory.getFactory(FFILongWritable.Factory.class, FFILongWritable.class);

    static FFILongWritable newInstance() {
        return factory.create();
    }

    @FFISetter
    void value(long val);

    @FFIGetter
    long value();

    @FFIFactory
    interface Factory {
        FFILongWritable create();
    }
}
