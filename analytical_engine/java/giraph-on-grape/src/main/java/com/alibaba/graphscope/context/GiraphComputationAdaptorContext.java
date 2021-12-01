package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

@SuppressWarnings("rawtypes")
public class GiraphComputationAdaptorContext implements DefaultContextBase {

    @Override
    public void Init(SimpleFragment frag, DefaultMessageManager messageManager,
        JSONObject jsonObject) {

    }

    @Override
    public void Output(SimpleFragment frag) {

    }
}
