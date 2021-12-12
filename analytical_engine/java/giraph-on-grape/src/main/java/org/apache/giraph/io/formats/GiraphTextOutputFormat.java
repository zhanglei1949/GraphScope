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

package org.apache.giraph.io.formats;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The text output format used for Giraph text writing.
 */
public abstract class GiraphTextOutputFormat
    extends TextOutputFormat<Text, Text> {

    /**
     * This function returns a record writer according to provided configuration. Giraph write file
     * to hdfs.
     *
     * In Giraph-on-grape, we write to local file system.
     *
     * @param job shall be null.
     * @return created record writer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
        throws IOException, InterruptedException {
//        Configuration configuration = job.getConfiguration();
        /**
         * In Giraph, they use job meta to determine the default file name.
         * We here name the file with appClassName + processId + time.
         */
//        String defaultFileName = configuration
//        return new LineRecordWriter<>();
        return null;
    }

    /**
     * This function is used to provide an additional path level to keep
     * different text outputs into different directories.
     *
     * @return  the subdirectory to be created under the output path
     */
    protected abstract String getSubdir();
}
