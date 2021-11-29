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
@FFITypeAlias("IntWritable")
public interface FFIIntWritable extends FFIPointer {
    Factory factory = FFITypeFactory.getFactory(FFIIntWritable.Factory.class, FFIIntWritable.class);

    static FFIIntWritable newInstance() {
        return factory.create();
    }

    @FFISetter
    void value(int val);

    @FFIGetter
    int value();

    @FFIFactory
    interface Factory {
        FFIIntWritable create();
    }
}
