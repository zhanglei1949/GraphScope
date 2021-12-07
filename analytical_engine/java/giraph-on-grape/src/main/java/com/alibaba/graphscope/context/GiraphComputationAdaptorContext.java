package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;


public class GiraphComputationAdaptorContext implements DefaultContextBase<Long,Long,Long,Double> {

    @Override
    public void Init(SimpleFragment<Long,Long,Long,Double> frag, DefaultMessageManager messageManager,
        JSONObject jsonObject) {

    }

    @Override
    public void Output(SimpleFragment<Long,Long,Long,Double> frag) {

    }
}
