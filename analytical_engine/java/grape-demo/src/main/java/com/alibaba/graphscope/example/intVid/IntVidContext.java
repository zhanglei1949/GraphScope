package com.alibaba.graphscope.example.intVid;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntVidContext extends VertexDataContext<IFragment<Integer, Integer, Double, Long>, Long>
        implements ParallelContextBase<Integer, Integer, Double, Long> {
    private static Logger logger = LoggerFactory.getLogger(IntVidContext.class);
    /**
     * Called by grape framework, before any PEval. You can initiating data structures need during
     * super steps here.
     *
     * @param frag           The graph fragment providing accesses to graph data.
     * @param messageManager The message manger which manages messages between fragments.
     * @param jsonObject     String args from cmdline.
     * @see IFragment
     * @see ParallelMessageManager
     * @see JSONObject
     */
    @Override
    public void Init(IFragment<Integer, Integer, Double, Long> frag, ParallelMessageManager messageManager, JSONObject jsonObject) {
        createFFIContext(frag, Long.class, false);
        logger.info("Initiating IntVidContext");
    }

    /**
     * Output will be executed when the computations finalizes. Data maintained in this context
     * shall be outputted here.
     *
     * @param frag The graph fragment contains the graph info.
     * @see IFragment
     */
    @Override
    public void Output(IFragment<Integer, Integer, Double, Long> frag) {

    }
}
