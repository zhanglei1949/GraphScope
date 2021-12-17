package org.apache.giraph.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class TaskAttemptContextImpl extends TaskAttemptContext {

    public TaskAttemptContextImpl(Configuration conf,
        TaskAttemptID taskId) {
        super(conf, taskId);
    }
}
