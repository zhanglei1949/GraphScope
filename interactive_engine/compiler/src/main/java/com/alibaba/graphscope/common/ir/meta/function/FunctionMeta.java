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

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class FunctionMeta {
    private final String signature;
    private final SqlReturnTypeInference returnTypeInference;
    private final SqlOperandTypeChecker operandTypeChecker;

    public FunctionMeta(
            String signature,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeChecker operandTypeChecker) {
        this.signature = signature;
        this.returnTypeInference = returnTypeInference;
        this.operandTypeChecker = operandTypeChecker;
    }

    public String getSignature() {
        return this.signature;
    }

    public SqlReturnTypeInference getReturnTypeInference() {
        return this.returnTypeInference;
    }

    public SqlOperandTypeChecker getOperandTypeChecker() {
        return this.operandTypeChecker;
    }
}
