package com.alibaba.graphscope.context;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.impl.GiraphDefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.GrapeReflectionUtils;
import com.alibaba.graphscope.utils.WritableFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.rmi.server.ObjID;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Communicator;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexFactory;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.graph.impl.CommunicatorImpl;
import org.apache.giraph.graph.impl.ImmutableEdgeManagerImpl;
import org.apache.giraph.graph.impl.VertexDataManagerImpl;
import org.apache.giraph.graph.impl.VertexIdManagerImpl;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.impl.DefaultWorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GiraphComputationAdaptorContext implements
    DefaultContextBase{
    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptorContext.class);

    public VertexRange<Long> innerVertices;
    private long innerVerticesNum;

    private WorkerContext workerContext;
    private AbstractComputation userComputation;
    public VertexImpl vertex;
    private GiraphMessageManager giraphMessageManager;
    /**
     * Communicator used for aggregation. Both AbstractComputation instance and workerContext will hold it.
     */
    private Communicator communicator;

    /**
     * Manages the vertex original id.
     */
    private VertexIdManager vertexIdManager;
    /**
     * Manages the vertex data.
     */
    private VertexDataManager vertexDataManager;

    /**
     * Edge manager. We can choose to use a immutable edge manager or others.
     */
    private EdgeManager edgeManager;


    public AbstractComputation getUserComputation() {
        return userComputation;
    }

    public GiraphMessageManager<LongWritable, LongWritable, DoubleWritable, LongWritable, LongWritable> getGiraphMessageManager() {
        return giraphMessageManager;
    }

    public WorkerContext getWorkerContext(){
        return workerContext;
    }

    private BitSet halted;

    @Override
    public void Init(SimpleFragment frag,
        DefaultMessageManager messageManager,
        JSONObject jsonObject) {

        /**
         * Construct a configuration obj.
         */
        ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = generateConfiguration(jsonObject);

        userComputation = (AbstractComputation) ReflectionUtils.newInstance(immutableClassesGiraphConfiguration.getComputationClass());
        workerContext = (WorkerContext) ReflectionUtils.newInstance(immutableClassesGiraphConfiguration.getWorkerContextClass());

        userComputation.setConf(immutableClassesGiraphConfiguration);

        logger.info("Created user computation class: " + userComputation.getClass().getName());
        innerVertices = frag.innerVertices();
        innerVerticesNum = frag.getInnerVerticesNum();

        giraphMessageManager = new GiraphDefaultMessageManager<>(frag, messageManager);
        userComputation.setGiraphMessageManager(giraphMessageManager);

        communicator = new CommunicatorImpl();
        userComputation.setCommunicator(communicator);
        /**
         * Important, we don't provided any constructors for workerContext, so make sure all fields
         * has been carefully set.
         */
        workerContext.setCommunicator(communicator);
        workerContext.setFragment(frag);
        workerContext.setCurStep(0);

        //parse user computation class and set oid, vdata, edata, inMsgType and outMsgType;
        initWritableFactory(userComputation);
        //parse fragment class to get oid,vid,vdata, edata,
        //TODO: If we follow GAE-ODPSGraph, then we will catch oid, vdata, edata all as refstring or constBlob

        //halt array to mark active
        halted = new BitSet((int) frag.getInnerVerticesNum());

        //Init vertex data/oid manager
        vertexDataManager = new VertexDataManagerImpl<LongWritable>(frag, innerVertices);
        vertexIdManager = new VertexIdManagerImpl<LongWritable>(frag, innerVertices);
        edgeManager = new ImmutableEdgeManagerImpl(frag);

        vertex = (VertexImpl<LongWritable, LongWritable, DoubleWritable>) VertexFactory
            .<LongWritable, LongWritable, DoubleWritable>createDefaultVertex(frag, this);
        vertex.setVertexIdManager(vertexIdManager);
        vertex.setVertexDataManager(vertexDataManager);
        vertex.setEdgeManager(edgeManager);
    }

    /**
     * For giraph applications, we need to run postApplication method after all computation.
     *
     * @param frag The graph fragment contains the graph info.
     */
    @Override
    public void Output(SimpleFragment frag) {
        workerContext.postApplication();

        //Output with vertexOutputClass
        //caution: do not use taskAttemptID
        TaskAttemptContext taskAttemptContext = new TaskAttemptContext(userComputation.getConf(), new TaskAttemptID());
        String filePath = userComputation.getConf().getDefaultWorkerFile();
        //TODO: remove this debug code
        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            Vertex<Long> grapeVertex = FFITypeFactoryhelper.newVertexLong();

            for (long i = 0; i < innerVerticesNum; ++i){
                grapeVertex.SetValue(i);
                bufferedWriter.write(vertexIdManager.getId(i) + "\t" + vertexDataManager.getVertexData(i));
            }
            bufferedWriter.close();
        }
        catch (Exception e){
            logger.error("Exception in writing out: " + e.getMessage());
        }
    }

    /**
     * TODO: remove this and use conf to create writables
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


    /**
     * return a configuration instance with key-value pairs in params.
     *
     * @param params received params.
     */
    private ImmutableClassesGiraphConfiguration generateConfiguration(JSONObject params){
        Configuration configuration = new Configuration();
        GiraphConfiguration giraphConfiguration =new GiraphConfiguration(configuration);

        try {
            ConfigurationUtils.parseArgs(giraphConfiguration, params);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
        }
        return new ImmutableClassesGiraphConfiguration<>(giraphConfiguration);
    }
}
