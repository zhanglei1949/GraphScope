package com.alibaba.graphscope.context;


import static org.apache.giraph.job.HadoopUtils.makeTaskAttemptContext;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.factory.GiraphComputationFactory;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.impl.GiraphDefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.Objects;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.Communicator;
import org.apache.giraph.graph.EdgeManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.giraph.graph.VertexFactory;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.giraph.graph.impl.AggregatorManagerImpl;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.apache.giraph.graph.impl.CommunicatorImpl;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic adaptor context class. The type parameter OID,VID_VDATA_T,EDATA_T is irrelevant to User
 * writables. They are type parameters for grape fragment. We need them to support multiple set of
 * actual type parameters in one adaptor.
 *
 * @param <OID_T>
 * @param <VID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 */
public class GiraphComputationAdaptorContext<OID_T, VID_T, VDATA_T, EDATA_T> implements
    DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphComputationAdaptorContext.class);

    private long innerVerticesNum;

    private WorkerContext workerContext;
    /** Only executed by the master, in our case, the coordinator worker in mpi world*/
    private MasterCompute masterCompute;
    private AbstractComputation userComputation;
    public VertexImpl vertex;
    private GiraphMessageManager giraphMessageManager;
    /**
     * Communicator used for aggregation. Both AbstractComputation instance and workerContext will
     * hold it.
     */
    private Communicator communicator;
    private FFICommunicator ffiCommunicator;

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

    /**
     * Aggregator manager.
     */
    private AggregatorManager aggregatorManager;


    public AbstractComputation getUserComputation() {
        return userComputation;
    }

    public GiraphMessageManager getGiraphMessageManager() {
        return giraphMessageManager;
    }

    public WorkerContext getWorkerContext() {
        return workerContext;
    }

    public boolean hasMasterCompute(){
        return Objects.nonNull(masterCompute);
    }

    public MasterCompute getMasterCompute(){
        return masterCompute;
    }

    public AggregatorManager getAggregatorManager(){
        return aggregatorManager;
    }

    private BitSet halted;

    @Override
    public void Init(SimpleFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
        DefaultMessageManager messageManager,
        JSONObject jsonObject) {

        /**
         * Construct a configuration obj.
         */
        ImmutableClassesGiraphConfiguration conf = generateConfiguration(
            jsonObject, frag);

        if (checkConsistency(conf)) {
            logger
                .info("Okay, the type parameters in user computation is consistent with fragment");
        } else {
            throw new IllegalStateException(
                "User computation type parameters not consistent with fragment types");
        }

        userComputation = (AbstractComputation) ReflectionUtils
            .newInstance(conf.getComputationClass());
	userComputation.setFragment(frag);
        userComputation.setConf(conf);

        logger.info("Created user computation class: " + userComputation.getClass().getName());
        innerVerticesNum = frag.getInnerVerticesNum();

//        communicator = new CommunicatorImpl();
//        userComputation.setCommunicator(communicator);
        /**
         * Important, we don't provided any constructors for workerContext, so make sure all fields
         * has been carefully set.
         */
//        workerContext = (WorkerContext) ReflectionUtils
//            .newInstance(conf.getWorkerContextClass());
        workerContext = conf.createWorkerContext();
//        workerContext.setCommunicator(communicator);
        workerContext.setFragment(frag);
        workerContext.setCurStep(0);
        userComputation.setWorkerContext(workerContext);

        //halt array to mark active
        halted = new BitSet((int) frag.getInnerVerticesNum());

        //Init vertex data/oid manager
        vertexDataManager = GiraphComputationFactory
            .createDefaultVertexDataManager(conf.getVertexValueClass(), conf.getGrapeOidClass(), conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(), conf.getGrapeEdataClass(),frag, innerVerticesNum,
                conf);
        vertexIdManager = GiraphComputationFactory
            .createDefaultVertexIdManager(conf.getVertexIdClass(), conf.getGrapeOidClass(), conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(), conf.getGrapeEdataClass(), frag, innerVerticesNum, conf);

//        edgeManager = new ImmutableEdgeManagerImpl(frag, vertexIdManager, conf);
        edgeManager = GiraphComputationFactory
            .createImmutableEdgeManagerImpl(conf.getVertexIdClass(),
                conf.getEdgeValueClass(), conf.getGrapeOidClass(), conf.getGrapeVidClass(),
                conf.getGrapeVdataClass(), conf.getGrapeEdataClass(), frag, vertexIdManager, conf);

        vertex = VertexFactory
            .createDefaultVertex(conf.getVertexIdClass(), conf.getVertexValueClass(),
                conf.getEdgeValueClass(), this);
        vertex.setVertexIdManager(vertexIdManager);
        vertex.setVertexDataManager(vertexDataManager);
        vertex.setEdgeManager(edgeManager);

        //VertexIdManager is needed since we need oid <-> lid converting.
        giraphMessageManager = new GiraphDefaultMessageManager<>(frag, messageManager,
            conf);
        userComputation.setGiraphMessageManager(giraphMessageManager);

        /** Aggregator manager, manages aggregation, reduce, broadcast */

//        String masterWorkerIp = getMasterWorkerIp(frag.fid(), frag.fnum());

//        aggregatorManager = new AggregatorManagerImpl(conf, frag.fid(), frag.fnum());
        aggregatorManager = new AggregatorManagerNettyImpl(conf, frag.fid(), frag.fnum());
        userComputation.setAggregatorManager(aggregatorManager);
        workerContext.setAggregatorManager(aggregatorManager);

        /**
         * Create master compute if master compute is specified.
         */
        if (conf.getMasterComputeClass() != null){
            masterCompute = conf.createMasterCompute();
            logger.info("Creating master compute class");
            try {
                masterCompute.setAggregatorManager(aggregatorManager);
                masterCompute.initialize();
                masterCompute.setFragment(frag);
                masterCompute.setConf(conf);
//                masterCompute.setOutgoingMessageClasses();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            logger.info("Finish master compute initialization.");
        }
        else {
            logger.info("No master compute class specified");
        }
    }

    /**
     * For giraph applications, we need to run postApplication method after all computation.
     *
     * @param frag The graph fragment contains the graph info.
     */
    @Override
    public void Output(SimpleFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        workerContext.postApplication();
        ImmutableClassesGiraphConfiguration conf = userComputation.getConf();
        String filePath = conf.getDefaultWorkerFile() + "-frag-" + frag.fid();
        //Output vertices.
        if (conf.hasVertexOutputFormat()){
            VertexOutputFormat vertexOutputFormat = conf.createWrappedVertexOutputFormat();
	    vertexOutputFormat.setConf(conf);
            TaskAttemptContext taskAttemptContext = makeTaskAttemptContext(conf);
            vertexOutputFormat.preWriting(taskAttemptContext);

            {
                try {
                    VertexWriter vertexWriter = vertexOutputFormat.createVertexWriter(taskAttemptContext);

                    vertexWriter.setConf(conf);
                    vertexWriter.initialize(taskAttemptContext);

                    //write vertex
                    Vertex vertex = conf.createVertex();
                    if (conf.getVertexIdClass().equals(VertexImpl.class)){
                        logger.info("Cast to vertexImpl to output");
                        VertexImpl vertexImp = (VertexImpl) vertex;
                        vertexImp.setVertexIdManager(vertexIdManager);
                        vertexImp.setVertexDataManager(vertexDataManager);
                        vertexImp.setEdgeManager(edgeManager);
                        vertexImp.setConf(conf);
                        for (long i = 0; i < innerVerticesNum; ++i) {
                            vertexImp.setLocalId((int)i);
                            vertexWriter.writeVertex(vertexImp);
                        }
                    }

                    vertexWriter.close(taskAttemptContext);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            vertexOutputFormat.postWriting(taskAttemptContext);


        }
        else {
            logger.info("No vertex output class specified, output using default output logic");

            try {
                logger.info("Writing output to: " + filePath);
                FileWriter fileWritter = new FileWriter(new File(filePath));
                BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
                logger.debug("inner vertices: " + innerVerticesNum + frag.innerVertices().size());
                for (long i = 0; i < innerVerticesNum; ++i) {
                    bufferedWriter.write(
                        vertexIdManager.getId(i) + "\t" + vertexDataManager.getVertexData(i) + "\n");
                }
                bufferedWriter.close();
            } catch (Exception e) {
                logger.error("Exception in writing out: " + e.getMessage());
            }
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

    public void activateVertex(long lid) {
        halted.set((int) lid, false);
    }


    /**
     * return a configuration instance with key-value pairs in params.
     *
     * @param params received params.
     */
    private ImmutableClassesGiraphConfiguration generateConfiguration(JSONObject params,
        SimpleFragment fragment) {
        Configuration configuration = new Configuration();
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration(configuration);

        try {
            ConfigurationUtils.parseArgs(giraphConfiguration, params);
//            ConfigurationUtils.parseJavaFragment(giraphConfiguration, fragment);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new ImmutableClassesGiraphConfiguration<>(giraphConfiguration, fragment);
    }

    /**
     * Check whether user provided giraph app consistent with our fragment.
     * @param configuration configuration
     * @return true if consistent.
     */
    private boolean checkConsistency(ImmutableClassesGiraphConfiguration configuration) {
        return ConfigurationUtils.checkTypeConsistency(configuration.getGrapeOidClass(),
            configuration.getVertexIdClass()) &&
            ConfigurationUtils.checkTypeConsistency(configuration.getGrapeEdataClass(),
                configuration.getEdgeValueClass()) &&
            ConfigurationUtils.checkTypeConsistency(configuration.getGrapeVdataClass(),
                configuration.getVertexValueClass());
    }


}
