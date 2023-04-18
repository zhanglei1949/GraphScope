package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppHeaderName.VINEYARD_ARROW_UTILS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFIStringProvider;
import com.alibaba.fastffi.FFIStringReceiver;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(VINEYARD_ARROW_UTILS_H)
@FFITypeAlias("vineyard::arrow_string_view")
public interface StringView extends FFIStringProvider, FFIStringReceiver, FFIPointer {

    long data();

    @CXXOperator("[]")
    byte byteAt(long index);

    long size();

    boolean empty();

    @FFIFactory
    interface Factory {
        StringView create();
    }
}
