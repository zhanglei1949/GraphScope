/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.function;

import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import org.apache.calcite.sql.type.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphFunctions {
    public static final String FUNCTION_PREFIX = "gs.function.";
    private final Map<String, FunctionMeta> functionMetaMap;

    public GraphFunctions() {
        this.functionMetaMap = Maps.newHashMap();
        this.registerBuiltInFunctions();
    }

    private void registerBuiltInFunctions() {
        String relationships = FUNCTION_PREFIX + "relationships";
        this.functionMetaMap.put(
                relationships,
                new FunctionMeta(
                        relationships,
                        // set return type inference of function 'gs.function.relationships'
                        ReturnTypes.ARG0
                                .andThen(
                                        (op, type) -> {
                                            GraphPathType.ElementType elementType =
                                                    (GraphPathType.ElementType)
                                                            type.getComponentType();
                                            return elementType.getExpandType();
                                        })
                                .andThen(SqlTypeTransforms.TO_ARRAY),
                        // // set operand type checker of function 'gs.function.relationships'
                        GraphOperandTypes.operandMetadata(
                                ImmutableList.of(GraphTypeFamily.PATH),
                                typeFactory -> ImmutableList.of(),
                                i -> "path",
                                i -> false)));
        String nodes = FUNCTION_PREFIX + "nodes";
        this.functionMetaMap.put(
                nodes,
                new FunctionMeta(
                        nodes,
                        ReturnTypes.ARG0
                                .andThen(
                                        (op, type) -> {
                                            GraphPathType.ElementType elementType =
                                                    (GraphPathType.ElementType)
                                                            type.getComponentType();
                                            return elementType.getGetVType();
                                        })
                                .andThen(SqlTypeTransforms.TO_ARRAY),
                        GraphOperandTypes.operandMetadata(
                                ImmutableList.of(GraphTypeFamily.PATH),
                                typeFactory -> ImmutableList.of(),
                                i -> "path",
                                i -> false)));
        String startNode = FUNCTION_PREFIX + "startNode";
        this.functionMetaMap.put(
                startNode,
                new FunctionMeta(
                        startNode,
                        ReturnTypes.ARG0.andThen(
                                (op, type) -> {
                                    GraphSchemaType edgeType = (GraphSchemaType) type;
                                    List<GraphLabelType.Entry> srcLabels =
                                            edgeType.getLabelType().getLabelsEntry().stream()
                                                    .map(
                                                            e ->
                                                                    new GraphLabelType.Entry()
                                                                            .label(e.getSrcLabel())
                                                                            .labelId(
                                                                                    e
                                                                                            .getSrcLabelId()))
                                                    .collect(Collectors.toList());
                                    return new GraphSchemaType(
                                            GraphOpt.Source.VERTEX,
                                            new GraphLabelType(srcLabels),
                                            ImmutableList.of());
                                }),
                        GraphOperandTypes.operandMetadata(
                                ImmutableList.of(GraphTypeFamily.EDGE),
                                typeFactory -> ImmutableList.of(),
                                i -> "path",
                                i -> false)));
        String endNode = FUNCTION_PREFIX + "endNode";
        this.functionMetaMap.put(
                endNode,
                new FunctionMeta(
                        endNode,
                        ReturnTypes.ARG0.andThen(
                                (op, type) -> {
                                    GraphSchemaType edgeType = (GraphSchemaType) type;
                                    List<GraphLabelType.Entry> endLabels =
                                            edgeType.getLabelType().getLabelsEntry().stream()
                                                    .map(
                                                            e ->
                                                                    new GraphLabelType.Entry()
                                                                            .label(e.getDstLabel())
                                                                            .labelId(
                                                                                    e
                                                                                            .getDstLabelId()))
                                                    .collect(Collectors.toList());
                                    return new GraphSchemaType(
                                            GraphOpt.Source.VERTEX,
                                            new GraphLabelType(endLabels),
                                            ImmutableList.of());
                                }),
                        GraphOperandTypes.operandMetadata(
                                ImmutableList.of(GraphTypeFamily.EDGE),
                                typeFactory -> ImmutableList.of(),
                                i -> "path",
                                i -> false)));
        String datetime = FUNCTION_PREFIX + "datetime";
        this.functionMetaMap.put(
                datetime,
                new FunctionMeta(
                        datetime,
                        ReturnTypes.TIMESTAMP,
                        GraphOperandTypes.operandMetadata(
                                ImmutableList.of(SqlTypeFamily.INTEGER),
                                typeFactory -> ImmutableList.of(),
                                i -> "timestamp",
                                i -> false),
                        InferTypes.explicit(
                                ImmutableList.of(
                                        StoredProcedureMeta.typeFactory.createSqlType(
                                                SqlTypeName.INTEGER)))));
    }

    public FunctionMeta getFunction(String functionName) {
        FunctionMeta meta = functionMetaMap.get(functionName);
        if (meta == null) {
            // if not exist, create a new function meta with no constraints on operand types and
            // return types
            meta =
                    new FunctionMeta(
                            functionName,
                            ReturnTypes.explicit(SqlTypeName.ANY),
                            OperandTypes.VARIADIC);
        }
        return meta;
    }
}
