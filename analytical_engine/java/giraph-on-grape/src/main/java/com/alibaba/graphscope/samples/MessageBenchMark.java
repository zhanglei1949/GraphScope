package com.alibaba.graphscope.samples;

import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Only send msg.
 */
public class MessageBenchMark extends
    BasicComputation<LongWritable, DoubleWritable, DoubleWritable, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(MessageApp.class);

    private static int MAX_SUPER_STEP = Integer.valueOf(System.getenv("MAX_SUPER_STEP"));

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
        if (getSuperstep() >= MAX_SUPER_STEP){
            vertex.voteToHalt();
            return ;
        }
        if (getSuperstep() > 1){
            int msgCnt = 0;
            for (LongWritable message : messages){
                msgCnt += 1;
            }
            if (vertex.getId().get() % 1000 == 0){
                logger.info("vertex: " + vertex.getId() + "receive msg size: " + msgCnt);
            }
            MessageBenchMarkWorkerContext.messageReceived += msgCnt;
        }
        LongWritable msg = new LongWritable(vertex.getId().get());
        MessageBenchMarkWorkerContext.messageSent += vertex.getNumEdges();
        sendMessageToAllEdges(vertex, msg);
        //logger.info("Vertex [" + vertex.getId() + "] send to all edges " +  vertex.getId());
    }

    public static class MessageBenchMarkWorkerContext extends WorkerContext{
        private static long messageSent = 0;
        private static long messageReceived = 0;

        /**
         * Initialize the WorkerContext. This method is executed once on each Worker before the
         * first superstep starts.
         *
         * @throws IllegalAccessException Thrown for getting the class
         * @throws InstantiationException Expected instantiation in this method.
         */
        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {

        }

        /**
         * Finalize the WorkerContext. This method is executed once on each Worker after the last
         * superstep ends.
         */
        @Override
        public void postApplication() {
            logger.info("after application: msg sent: " + messageSent + ", msg rcv: " + messageReceived);
        }

        /**
         * Execute user code. This method is executed once on each Worker before each superstep
         * starts.
         */
        @Override
        public void preSuperstep() {

        }

        /**
         * Execute user code. This method is executed once on each Worker after each superstep
         * ends.
         */
        @Override
        public void postSuperstep() {
            logger.info("after superstep: " + getSuperstep() + "msg sent: " + messageSent + ", msg rcv: " + messageReceived);
        }
    }
}
