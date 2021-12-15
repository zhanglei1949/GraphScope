/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;


/**
 * Helper static methods for working with Writable objects.
 */
public class WritableUtils {

    /**
     * Don't construct.
     */
    private WritableUtils() {
    }

    /**
     * Instantiate a new Writable, checking for NullWritable along the way.
     *
     * @param klass Class
     * @param <W>   type
     * @return new instance of class
     */
    public static <W extends Writable> W createWritable(Class<W> klass) {
        return createWritable(klass, null);
    }

    /**
     * Instantiate a new Writable, checking for NullWritable along the way.
     *
     * @param klass         Class
     * @param configuration Configuration
     * @param <W>           type
     * @return new instance of class
     */
    public static <W extends Writable> W createWritable(
        Class<W> klass,
        ImmutableClassesGiraphConfiguration configuration) {
        W result;
        if (NullWritable.class.equals(klass)) {
            result = (W) NullWritable.get();
        } else {
            result = ReflectionUtils.newInstance(klass);
        }
        ConfigurationUtils.configureIfPossible(result, configuration);
        return result;
    }

    /**
     * Read class from data input. Matches {@link #writeClass(Class, DataOutput)}.
     *
     * @param input Data input
     * @param <T>   Class type
     * @return Class, or null if null was written
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> readClass(DataInput input) throws IOException {
        if (input.readBoolean()) {
            String className = input.readUTF();
            try {
                return (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("readClass: No class found " +
                    className);
            }
        } else {
            return null;
        }
    }

    /**
     * Write class to data output. Also handles the case when class is null.
     *
     * @param clazz  Class
     * @param output Data output
     * @param <T>    Class type
     */
    public static <T> void writeClass(Class<T> clazz,
        DataOutput output) throws IOException {
        output.writeBoolean(clazz != null);
        if (clazz != null) {
            output.writeUTF(clazz.getName());
        }
    }

    /**
     * Write object to output stream
     * @param object Object
     * @param output Output stream
     * @throws IOException
     */
    public static void writeWritableObject(
        Writable object, DataOutput output)
        throws IOException {
        output.writeBoolean(object != null);
        if (object != null) {
            output.writeUTF(object.getClass().getName());
            object.write(output);
        }
    }

    /**
     * Reads object from input stream
     * @param input Input stream
     * @param conf Configuration
     * @param <T> Object type
     * @return Object
     * @throws IOException
     */
    public static <T extends Writable>
    T readWritableObject(DataInput input,
        ImmutableClassesGiraphConfiguration conf) throws IOException {
        if (input.readBoolean()) {
            String className = input.readUTF();
            try {
                T object =
                    (T) ReflectionUtils.newInstance(Class.forName(className), conf);
                object.readFields(input);
                return object;
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("readWritableObject: No class found " +
                    className);
            }
        } else {
            return null;
        }
    }
}