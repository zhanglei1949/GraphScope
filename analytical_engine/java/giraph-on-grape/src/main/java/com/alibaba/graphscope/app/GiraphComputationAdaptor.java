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
@SuppressWarnings("rawtypes")
public class GiraphComputationAdaptor implements DefaultAppBase {

    @Override
    public void PEval(SimpleFragment graph, DefaultContextBase context,
        DefaultMessageManager messageManager) {

    }

    @Override
    public void IncEval(SimpleFragment graph, DefaultContextBase context,
        DefaultMessageManager messageManager) {

    }
}
