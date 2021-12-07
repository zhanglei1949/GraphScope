package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

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

    }
}
