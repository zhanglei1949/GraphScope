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

package com.alibaba.graphscope.annotation;

import static com.alibaba.graphscope.utils.CppClassName.DOUBLE_MSG;
import static com.alibaba.graphscope.utils.CppClassName.LONG_MSG;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFunGen;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGenBatch;
import com.alibaba.graphscope.utils.CppClassName;

/**
 * In <em>GRAPE-jdk</em>, we split programming interfaces from actual implementation. As the
 * programming interfaces are defined in grape-jdk, the actually implementation code generated with
 * annotation processors. Each FFIGen defines a generation schema for a FFIPointer interface.
 */
@FFIGenBatch(
        value = {
            @FFIGen(type = "com.alibaba.graphscope.ds.EmptyType"),
//            @FFIGen(
//                    type = "com.alibaba.graphscope.stdcxx.StdVector",
//                    templates = {
//                        @CXXTemplate(cxx = "int", java = "Integer"),
//                        @CXXTemplate(cxx = "char", java = "Byte"),
//                        @CXXTemplate(cxx = "std::vector<int>", java = "com.alibaba.graphscope.stdcxx.StdVector<java.lang.Integer>"),
//                        @CXXTemplate(cxx = "std::vector<char>", java = "com.alibaba.graphscope.stdcxx.StdVector<java.lang.Byte>")
//                    }),
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.DoubleMsg"),
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.LongMsg"),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.message.PrimitiveMessage",
                    templates = {
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int64_t", java = "Long")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.Vertex",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.VertexRange",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.VertexArray",
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int32_t", "uint64_t"},
                                java = {"Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"double", "uint64_t"},
                                java = {"Double", "Long"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"uint32_t", "uint64_t"},
                                java = {"Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"double", "uint64_t"},
                                java = {"Double", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GrapeNbr",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GrapeAdjList",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GSVertexArray",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment",
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "double"},
                                java = {"Long", "Long", "Long", "Double"})
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.context.ffi.FFIVertexDataContext",
                    templates = {
                        @CXXTemplate(
                                cxx = {
                                    CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,double>",
                                    "int64_t"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                    "Long"
                                },
                                include = @CXXHead(GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H)),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.DefaultMessageManager",
                    functionTemplates = {
                        @FFIFunGen(
                            name = "sendToImmutableFragment",
                            returnType = "void",
                            parameterTypes = {"FRAG_T", "MSG_T"},
                            templates = {
                                @CXXTemplate(
                                    cxx = {
                                        CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,double>",
                                        "std::vector<char>"
                                    },
                                    java = {
                                        "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                        "com.alibaba.graphscope.stdcxx.FFIByteVector"
                                    }
                                )
                            }
                        ),
                        @FFIFunGen(
                            name = "getPureMessage",
                            returnType = "boolean",
                            parameterTypes = {"MSG_T"},
                            templates = {
                                @CXXTemplate(
                                    cxx = {
                                        "std::vector<char>"
                                    },
                                    java = {
                                        "com.alibaba.graphscope.stdcxx.FFIByteVector"
                                    }
                                )
                            }
                        ),
                        @FFIFunGen(
                                name = "sendMsgThroughIEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,double>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughOEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,double>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,double>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "syncStateOnOuterVertex",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,double>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "getMessage",
                                returnType = "boolean",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,double>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.communication.FFICommunicator",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "sum",
                                returnType = "void",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = DOUBLE_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.DoubleMsg"),
                                    @CXXTemplate(
                                            cxx = LONG_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.LongMsg")
                                }),
                        @FFIFunGen(
                                name = "min",
                                returnType = "void",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = DOUBLE_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.DoubleMsg"),
                                    @CXXTemplate(
                                            cxx = LONG_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.LongMsg")
                                }),
                        @FFIFunGen(
                                name = "max",
                                returnType = "void",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = DOUBLE_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.DoubleMsg"),
                                    @CXXTemplate(
                                            cxx = LONG_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.LongMsg")
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.MessageInBuffer",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "getMessage",
                                parameterTypes = {"FRAG_T,MSG_T"},
                                returnType = "boolean",
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.GRAPE_IMMUTABLE_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,jdouble>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                })
                    })
        })
public class AnnotationInvoker {}
