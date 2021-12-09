package com.alibaba.graphscope.samples;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Only send msg.
 */
public class MessageApp extends
    BasicComputation<LongWritable, LongWritable, DoubleWritable, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(MessageApp.class);

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep.  Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(Vertex<LongWritable, LongWritable, DoubleWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
        if (getSuperstep() == 0)
        for (LongWritable message : messages) {
            if (message.get() != 123) {
                logger.error("Vertex: " + vertex.getId().get() + "receive msg: " + message.get() +", which is impossible.");
                break;
            }
        }

        sendMessageToAllEdges(vertex, new LongWritable(123));
        vertex.voteToHalt();
    }
}
