package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.MessageIterable;
import java.io.IOException;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.graphscope.ds.VertexRange;

/**
 * This adaptor bridges c++ driver app and Giraph Computation.
 *
 * <p>
 * Using raw types since we are not aware of Computation type parameters at this time.
 * </p>
 */
public class GiraphComputationAdaptor implements
    DefaultAppBase<Long, Long, Long, Double, GiraphComputationAdaptorContext> {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptor.class);

    /**
     * Partial Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see SimpleFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    @Override
    public void PEval(SimpleFragment<Long, Long, Long, Double> graph,
        DefaultContextBase<Long, Long, Long, Double> context,
        DefaultMessageManager messageManager) {

        GiraphComputationAdaptorContext ctx = (GiraphComputationAdaptorContext) context;
        AbstractComputation<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> userComputation = ctx
            .getUserComputation();
        GiraphMessageManager<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> giraphMessageManager = ctx
            .getGiraphMessageManager();

        //In first round, there is no message, we pass an empty iterable.
        Iterable<LongWritable> messages = new MessageIterable<>();

        //TODO: remove this debug code
        VertexDataManager vertexDataManager = ctx.vertex.getVertexDataManager();
        VertexIdManager vertexIdManager = ctx.vertex.getVertexIdManager();
        for (Vertex<Long> grapeVertex : ctx.innerVertices.locals()) {
            if (grapeVertex.GetValue().intValue() < 10) {
                logger.info(
                    "Vertex: " + grapeVertex.GetValue().intValue() + ", oid: " + vertexIdManager
                        .getId(grapeVertex.GetValue()) + ", vdata: " + vertexDataManager
                        .getVertexData(grapeVertex.GetValue()));
            }
        }

        try {
            for (Vertex<Long> grapeVertex : ctx.innerVertices.locals()) {
                ctx.vertex.setLocalId(grapeVertex.GetValue().intValue());
                userComputation.compute(ctx.vertex, messages);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //wait msg send finish.
        giraphMessageManager.finishMessageSending();

        //We can not judge whether to proceed by messages sent and check halted array.
        if (giraphMessageManager.anyMessageToSelf()){
            messageManager.ForceContinue();
        }

        //do aggregation.
    }

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see SimpleFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    @Override
    public void IncEval(SimpleFragment<Long, Long, Long, Double> graph,
        DefaultContextBase<Long, Long, Long, Double> context,
        DefaultMessageManager messageManager) {

        GiraphComputationAdaptorContext ctx = (GiraphComputationAdaptorContext) context;
        AbstractComputation<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> userComputation = ctx
            .getUserComputation();
        GiraphMessageManager<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> giraphMessageManager = ctx
            .getGiraphMessageManager();
        //0. receive messages
        giraphMessageManager.receiveMessages();

        //1. compute
        try {
	    logger.info("range: " + ctx.innerVertices.begin().GetValue() + " " + ctx.innerVertices.end().GetValue() + "addr: " + ctx.innerVertices.getAddress());
	    VertexRange<Long> innerVertices = graph.innerVertices();
	    logger.info("this range: " + innerVertices.begin().GetValue() + " " + innerVertices.end().GetValue() + "addr: " + innerVertices.getAddress());
            for (Vertex<Long> grapeVertex : innerVertices.locals()) {
                int lid = grapeVertex.GetValue().intValue();
                if (giraphMessageManager.messageAvailable(lid)){
                    ctx.activateVertex(lid); //set halted[lid] to false;
                }
                if (!ctx.isHalted(lid)) {
                    ctx.vertex.setLocalId(lid);
                    userComputation.compute(ctx.vertex, giraphMessageManager.getMessages(lid));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //2. send msg
        giraphMessageManager.finishMessageSending();

        if (giraphMessageManager.anyMessageToSelf()) {
            messageManager.ForceContinue();
        }

    }
}
