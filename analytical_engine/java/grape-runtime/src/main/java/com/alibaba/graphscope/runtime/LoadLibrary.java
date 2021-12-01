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

package com.alibaba.graphscope.runtime;

/** Load JNI library with library name. */
public class LoadLibrary {
    static {
        System.loadLibrary("grape-jni");
    }
    /**
     * Loading the library with library name.
     *
     * @param userLibrary name for library to be loaded.
     */
    public static void invoke(String userLibrary) {
        System.out.println("loading " + userLibrary);
        System.loadLibrary(userLibrary);
    }
}
