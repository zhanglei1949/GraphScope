package com.alibaba.graphscope.context;

import static com.alibaba.graphscope.utils.Flags.APP_CLASS;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.impl.GiraphDefaultMessageManager;
import com.alibaba.graphscope.utils.GrapeReflectionUtils;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.VertexFactory;
import org.apache.giraph.graph.VertexImpl;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GiraphComputationAdaptorContext implements
    DefaultContextBase<Long, Long, Long, Double> {

    private AbstractComputation<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> userComputation;
    public VertexImpl<LongWritable, LongWritable, DoubleWritable> vertex;
    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptorContext.class);
    public VertexRange<Long> innerVertices;
    private GiraphMessageManager<LongWritable, LongWritable, DoubleWritable,LongWritable,LongWritable> giraphMessageManager;

    public AbstractComputation<LongWritable,LongWritable,DoubleWritable,LongWritable,LongWritable> getUserComputation(){
        return userComputation;
    }
    public GiraphMessageManager<LongWritable, LongWritable, DoubleWritable,LongWritable,LongWritable> getGiraphMessageManager(){
        return giraphMessageManager;
    }

    @Override
    public void Init(SimpleFragment<Long, Long, Long, Double> frag,
        DefaultMessageManager messageManager,
        JSONObject jsonObject) {
        String userComputationClass = jsonObject.getString(APP_CLASS);
        if (userComputationClass.isEmpty()) {
            logger.error("Empty app class");
            return;
        }
        userComputation = GrapeReflectionUtils.loadAndCreate(userComputationClass);
        logger.info("Created user computation class: " + userComputation.getClass().getName());
        innerVertices = frag.innerVertices();

        vertex = (VertexImpl<LongWritable, LongWritable, DoubleWritable>) VertexFactory
            .<LongWritable, LongWritable, DoubleWritable>createDefaultVertex(frag);

        giraphMessageManager = new GiraphDefaultMessageManager<>(frag, messageManager);
        userComputation.setGiraphMessageManager(giraphMessageManager);
    }

    @Override
    public void Output(SimpleFragment<Long, Long, Long, Double> frag) {

    }
}
