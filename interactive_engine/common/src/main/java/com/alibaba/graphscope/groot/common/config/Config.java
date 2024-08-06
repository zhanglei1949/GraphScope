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
package com.alibaba.graphscope.groot.common.config;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;

import java.util.function.Function;

public class Config<T> {
    private final String key;
    private final String defaultVal;

    private final Function<String, T> parseFunc;

    public Config(String key, String defaultVal, Function<String, T> parseFunc) {
        this.key = key;
        this.defaultVal = defaultVal;
        this.parseFunc = parseFunc;
    }

    public T get(Configs configs) {
        String valStr = configs.get(key, defaultVal);
        try {
            T val = parseFunc.apply(valStr);
            return val;
        } catch (Exception e) {
            throw new InvalidArgumentException(
                    "key [" + key + "] val [" + valStr + "] parse failed", e);
        }
    }

    public static Config<Short> shortConfig(String key, short defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), Short::parseShort);
    }

    public static Config<Integer> intConfig(String key, int defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), Integer::parseInt);
    }

    public static Config<Long> longConfig(String key, long defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), Long::parseLong);
    }

    public static Config<String> stringConfig(String key, String defaultVal) {
        return new Config<>(key, defaultVal, Function.identity());
    }

    public static Config<Boolean> boolConfig(String key, boolean defaultVal) {
        return new Config<>(key, String.valueOf(defaultVal), Boolean::parseBoolean);
    }

    public String getKey() {
        return this.key;
    }
}
