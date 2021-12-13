package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.MessageIterable;
import java.io.IOException;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This adaptor bridges c++ driver app and Giraph Computation.
 *
 * <p>
 * Using raw types since we are not aware of Computation type parameters at this time.
 * </p>
 */
public class GiraphComputationAdaptor implements
    DefaultAppBase {

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
    public void PEval(SimpleFragment graph,
        DefaultContextBase context,
        DefaultMessageManager messageManager) {

        GiraphComputationAdaptorContext ctx = (GiraphComputationAdaptorContext) context;
        AbstractComputation userComputation = ctx
            .getUserComputation();
        GiraphMessageManager giraphMessageManager = ctx
            .getGiraphMessageManager();
        WorkerContext workerContext = ctx.getWorkerContext();
        //Before computation, we execute preparation methods provided by user's worker context.
        try {
            workerContext.preApplication();
        } catch (Exception e) {
            logger.error("Exception in workerContext preApplication: " + e.getMessage());
            return;
        }

        workerContext.preSuperstep();

        //In first round, there is no message, we pass an empty iterable.
//        Iterable<LongWritable> messages = new MessageIterable<>();
        Iterable<Writable> messages = new MessageIterable<>();

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
        //PostStep should run before finish message sending
        workerContext.postSuperstep();

        //wait msg send finish.
        giraphMessageManager.finishMessageSending();

        //increase super step
        userComputation.incStep();
        workerContext.setCurStep(1);

        //We can not judge whether to proceed by messages sent and check halted array.
        if (giraphMessageManager.anyMessageToSelf()) {
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
    public void IncEval(SimpleFragment graph,
        DefaultContextBase context,
        DefaultMessageManager messageManager) {

        GiraphComputationAdaptorContext ctx = (GiraphComputationAdaptorContext) context;
        AbstractComputation userComputation = ctx
            .getUserComputation();
        GiraphMessageManager giraphMessageManager = ctx
            .getGiraphMessageManager();
        WorkerContext workerContext = ctx.getWorkerContext();
        //Worker context
        workerContext.preSuperstep();

        //0. receive messages
        giraphMessageManager.receiveMessages();

        //1. compute
        try {
            logger.info(
                "range: " + ctx.innerVertices.begin().GetValue() + " " + ctx.innerVertices.end()
                    .GetValue() + "addr: " + ctx.innerVertices.getAddress());
            VertexRange<Long> innerVertices = graph.innerVertices();
            logger.info(
                "this range: " + innerVertices.begin().GetValue() + " " + innerVertices.end()
                    .GetValue() + "addr: " + innerVertices.getAddress());
            for (Vertex<Long> grapeVertex : innerVertices.locals()) {
                int lid = grapeVertex.GetValue().intValue();
                if (giraphMessageManager.messageAvailable(lid)) {
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
        workerContext.postSuperstep();

        //increase super step
        userComputation.incStep();
        //Also increase worker context.
        workerContext.setCurStep((int) userComputation.getSuperstep());

        //2. send msg
        giraphMessageManager.finishMessageSending();

        if (giraphMessageManager.anyMessageToSelf()) {
            messageManager.ForceContinue();
        }
    }
}
