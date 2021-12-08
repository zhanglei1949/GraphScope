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
import com.alibaba.graphscope.utils.WritableFactory;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.rmi.server.ObjID;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexFactory;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.graph.impl.VertexDataManagerImpl;
import org.apache.giraph.graph.impl.VertexIdManagerImpl;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GiraphComputationAdaptorContext implements
    DefaultContextBase<Long, Long, Long, Double> {

    public VertexRange<Long> innerVertices;

    private AbstractComputation<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> userComputation;
    public VertexImpl<LongWritable, LongWritable, DoubleWritable> vertex;
    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptorContext.class);
    private long innerVerticesNum;
    private GiraphMessageManager<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> giraphMessageManager;

    /**
     * Manages the vertex original id.
     */
    private VertexIdManager vertexIdManager;
    /**
     * Manages the vertex data.
     */
    private VertexDataManager vertexDataManager;

    public AbstractComputation<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> getUserComputation() {
        return userComputation;
    }

    public GiraphMessageManager<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> getGiraphMessageManager() {
        return giraphMessageManager;
    }

    private BitSet halted;

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
        innerVerticesNum = frag.getInnerVerticesNum();



        giraphMessageManager = new GiraphDefaultMessageManager<>(frag, messageManager);
        userComputation.setGiraphMessageManager(giraphMessageManager);

        //parse user computation class and set oid, vdata, edata, inMsgType and outMsgType;
        initWritableFactory(userComputation);

        //halt array to mark active
        halted = new BitSet((int) frag.getInnerVerticesNum());

        //Init vertex data/oid manager
        vertexDataManager = new VertexDataManagerImpl<LongWritable>(frag, innerVertices);
        vertexIdManager = new VertexIdManagerImpl<LongWritable>(frag, innerVertices);

        vertex = (VertexImpl<LongWritable, LongWritable, DoubleWritable>) VertexFactory
            .<LongWritable, LongWritable, DoubleWritable>createDefaultVertex(frag, this);
        vertex.setVertexIdManager(vertexIdManager);
        vertex.setVertexDataManager(vertexDataManager);
    }

    @Override
    public void Output(SimpleFragment<Long, Long, Long, Double> frag) {

    }

    /**
     * User app extends abstract computation, so we use reflection to find it type parameters.
     * Notice that user can extend {@link AbstractComputation} and ${@link
     * org.apache.giraph.graph.BasicComputation}, we need to take both of them into consideration.
     *
     * @param userComputation instance OF USER app
     */
    private void initWritableFactory(AbstractComputation userComputation) {
        Class<? extends AbstractComputation> userComputationClz = userComputation.getClass();
        Type genericType = userComputationClz.getGenericSuperclass();
        // System.out.println(genericType[0].getTypeName());
        if (!(genericType instanceof ParameterizedType)) {
            logger.error("not parameterize type");
            return;
        }
        Type[] typeParams = ((ParameterizedType) genericType).getActualTypeArguments();
        if (typeParams.length > 5 || typeParams.length < 4) {
            logger.error(
                "number of params doesn't match, should be 5, acutual is" + typeParams.length);
            return;
        }
        List<Class<?>> clzList = new ArrayList<>(5);
        for (int i = 0; i < typeParams.length; ++i) {
            clzList.add((Class<?>) typeParams[i]);
        }
        WritableFactory.setOidClass((Class<? extends WritableComparable>) clzList.get(0));
        WritableFactory.setInMsgClass((Class<? extends Writable>) clzList.get(3));
        if (typeParams.length == 4) {
            WritableFactory.setOutMsgClass((Class<? extends Writable>) clzList.get(3));
        } else {
            WritableFactory.setOutMsgClass((Class<? extends Writable>) clzList.get(4));
        }
    }

    public void haltVertex(long lid) {
        halted.set((int) lid);
    }

    public boolean isHalted(long lid) {
        return halted.get((int) lid);
    }

    public boolean allHalted() {
        return halted.cardinality() == innerVerticesNum;
    }
    public void activateVertex(long lid){
        halted.set((int) lid, false);
    }
}
