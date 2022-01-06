package com.alibaba.graphscope.samples;

import java.io.IOException;
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
    BasicComputation<LongWritable, DoubleWritable, DoubleWritable, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(MessageApp.class);

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep.  Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            logger.info("There should be no messages in step0, " + vertex.getId());
            boolean flag = false;
            for (LongWritable message : messages){
                flag = true;
            }
            if (flag){
                throw new IllegalStateException("Expect no msg received in step 1, but actually received");
            }
            int repeatSendTimes = 10;
            for (int i = 0; i < repeatSendTimes; ++i){
                sendMessageToAllEdges(vertex, new LongWritable(vertex.getId().get()));
            }
            logger.info("Vertex [" + vertex.getId() + "] send to all edges " +  vertex.getId());
        }
        else if (getSuperstep() == 1){
            logger.info("Checking received msg");
            boolean flag = false;
            for (LongWritable message : messages){
                logger.info("Received msg: " + message);
            }
            vertex.voteToHalt();
        }
        else {
            logger.info("Impossible: " + getSuperstep());
        }
    }
}
