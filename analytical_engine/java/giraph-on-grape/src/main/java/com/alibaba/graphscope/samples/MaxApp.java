package com.alibaba.graphscope.samples;

import java.io.IOException;
import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class MaxApp extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {


    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep.  Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
        boolean changed = false;
        for (IntWritable message : messages) {
            if (vertex.getValue().get() < message.get()) {
                vertex.setValue(message);
                changed = true;
            }
        }
        if (getSuperstep() == 0 || changed) {
            sendMessageToAllEdges(vertex, vertex.getValue());
        }
        vertex.voteToHalt();
    }
}
