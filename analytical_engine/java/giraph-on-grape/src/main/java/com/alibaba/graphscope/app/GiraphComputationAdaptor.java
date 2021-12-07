package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import java.io.IOException;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * This adaptor bridges c++ driver app and Giraph Computation.
 *
 * <p>
 *     Using raw types since we are not aware of Computation type parameters at this time.
 * </p>
 */
public class GiraphComputationAdaptor implements DefaultAppBase<Long,Long,Long,Double,GiraphComputationAdaptorContext> {

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
        AbstractComputation<LongWritable,LongWritable, DoubleWritable,LongWritable,LongWritable> userComputation = ctx.getUserComputation();
        try {
            for (Vertex<Long> grapeVertex : ctx.innerVertices){
                ctx.vertex.setLocalId(grapeVertex.GetValue().intValue());
                userComputation.compute(ctx.vertex, null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        AbstractComputation<LongWritable,LongWritable, DoubleWritable,LongWritable,LongWritable> userComputation = ctx.getUserComputation();
        GiraphMessageManager<LongWritable,LongWritable, DoubleWritable, LongWritable,LongWritable> giraphMessageManager = ctx.getGiraphMessageManager();
        //0. Do aggregation.

        //1. compute
        try {
            for (Vertex<Long> grapeVertex : ctx.innerVertices){
                ctx.vertex.setLocalId(grapeVertex.GetValue().intValue());
                userComputation.compute(ctx.vertex, giraphMessageManager.getMessages(ctx.vertex));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
