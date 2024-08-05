package com.alibaba.graphscope.example.intVid;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntVidTest implements ParallelAppBase<Integer, Integer, Double, Long, IntVidContext> {
    private static Logger logger = LoggerFactory.getLogger(IntVidTest.class);

    /**
     * Partial Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void PEval(IFragment<Integer, Integer, Double, Long> graph, ParallelContextBase<Integer, Integer, Double, Long> context, ParallelMessageManager messageManager) {
        logger.info("IntVidTest PEval");
        messageManager.forceContinue();
    }

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void IncEval(IFragment<Integer, Integer, Double, Long> graph, ParallelContextBase<Integer, Integer, Double, Long> context, ParallelMessageManager messageManager) {
        logger.info("IntVidTest IncEval");
    }
}
