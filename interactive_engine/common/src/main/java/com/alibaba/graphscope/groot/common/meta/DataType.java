/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.common.meta;

import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.common.exception.InternalException;
import com.alibaba.graphscope.groot.common.exception.InvalidDataTypeException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

/**
 * @author beimian
 * 2018/05/06
 */
@JsonSerialize(using = DataTypeSerializer.class)
@JsonDeserialize(using = DataTypeDeserializer.class)
public class DataType {

    // for compatible
    public static final DataType BOOL = new DataType(InternalDataType.BOOL);
    public static final DataType CHAR = new DataType(InternalDataType.CHAR);
    public static final DataType SHORT = new DataType(InternalDataType.SHORT);
    public static final DataType INT = new DataType(InternalDataType.INT);
    public static final DataType LONG = new DataType(InternalDataType.LONG);
    public static final DataType FLOAT = new DataType(InternalDataType.FLOAT);
    public static final DataType DOUBLE = new DataType(InternalDataType.DOUBLE);
    public static final DataType BYTES = new DataType(InternalDataType.BYTES);
    public static final DataType STRING = new DataType(InternalDataType.STRING);

    // See also: `Date32` in common.proto.
    public static final DataType DATE = new DataType(InternalDataType.DATE);
    // See also: `Time32` in common.proto.
    public static final DataType TIME = new DataType(InternalDataType.TIME);
    // See also: `Timestamp` in common.proto.
    public static final DataType TIMESTAMP = new DataType(InternalDataType.TIMESTAMP);

    // For LIST, SET and MAP
    @JsonProperty private String expression;
    @JsonProperty private InternalDataType type;

    public DataType(InternalDataType internalDataType) {
        this.type = internalDataType;
    }

    public static DataType toDataType(int i) {
        return new DataType(InternalDataType.values()[i]);
    }

    public static DataType valueOf(String typeName) {
        if (typeName.startsWith("DATE")) {
            return DATE;
        }
        if (typeName.startsWith("TIME")) {
            return TIME;
        }
        if (typeName.startsWith("TIMESTAMP")) {
            return TIMESTAMP;
        }
        return new DataType(InternalDataType.valueOf(typeName));
    }

    public boolean isInt() {
        return this.type == InternalDataType.SHORT
                || this.type == InternalDataType.INT
                || this.type == InternalDataType.LONG;
    }

    public void setExpression(String expression) throws GrootException {
        if (isPrimitiveType()) {
            this.expression = null;
            return;
        }

        if (!isValid(expression)) {
            throw new InvalidDataTypeException(
                    "expression is not valid, subType "
                            + "must be primitiveTypes: "
                            + InternalDataType.primitiveTypes);
        }
        this.expression = expression;
    }

    public String getExpression() {
        return this.expression;
    }

    public InternalDataType getType() {
        return type;
    }

    @JsonIgnore
    public String name() {
        return this.getType().name();
    }

    @JsonValue
    public String getJson() {
        return this.type.name() + (this.expression.isEmpty() ? "" : "<" + this.expression + ">");
    }

    public boolean isValid(String expression) {
        if (this.type == InternalDataType.SET || this.type == InternalDataType.LIST) {
            return validSubTypes(expression);
        } else if (this.type == InternalDataType.MAP) {
            String s = expression.replaceAll("[ ]*,[ ]*", ",");
            String[] split = s.split(",");
            if (split.length < 2) {
                return false;
            } else {
                return validSubTypes(split[0]) && validSubTypes(split[1]);
            }
        }
        return true;
    }

    public boolean validSubTypes(String expression) {
        return InternalDataType.primitiveTypes.contains(expression.trim().toUpperCase());
    }

    public boolean isPrimitiveType() {
        return !(this.type == InternalDataType.LIST
                || this.type == InternalDataType.MAP
                || this.type == InternalDataType.SET);
    }

    public boolean isFixedLen() {
        switch (this.type) {
            case BOOL:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public int getFixedSize() {
        switch (this.type) {
            case BOOL:
            case CHAR:
                return 1;
            case SHORT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case LONG:
            case DOUBLE:
                return 8;
            default:
                throw new InternalException("unreachable!");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataType)) return false;
        DataType dataType = (DataType) o;
        // a == b || (a != null && a.equals(b))
        return Objects.equals(getExpression(), dataType.getExpression())
                && getType() == dataType.getType();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getExpression() + getType());
    }

    @Override
    public String toString() {
        return "DataType{" + "expression='" + expression + '\'' + ", type=" + type + '}';
    }
}
