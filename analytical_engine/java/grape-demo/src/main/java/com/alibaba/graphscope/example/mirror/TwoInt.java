/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.example.mirror;

import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIJava;
import com.alibaba.fastffi.FFIMirror;
import com.alibaba.fastffi.FFINameSpace;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * GRAPE-jdk allows user to define their own data structures with DSL, @FFIMirror provided by
 * fastFFI. The defined data structure are stored and managed in c++ memory. Here is the sample
 * usage for Defining a Class with two int fields.
 */
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("TwoInt")
public interface TwoInt extends FFIPointer, FFIJava {
    @FFIGetter
    int intField1();

    @FFIGetter
    int intField2();
}
