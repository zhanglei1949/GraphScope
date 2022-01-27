package com.alibaba.graphscope.loader.impl;

import static com.alibaba.graphscope.loader.LoaderUtils.checkFileExist;
import static com.alibaba.graphscope.loader.LoaderUtils.getNumLinesOfFile;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.loader.GraphDataBufferManager;
import com.alibaba.graphscope.loader.LoaderBase;
import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load from a file on system.
 */
public class FileLoader implements LoaderBase {

    private static Logger logger = LoggerFactory.getLogger(FileLoader.class);
    private static int threadNum;
    private static Class<? extends VertexInputFormat> inputFormatClz;
    private static Class<? extends VertexReader> vertexReaderClz;
    private static VertexInputFormat vertexInputFormat;
    private static VertexReader vertexReader;
    private static ExecutorService executor;
//    private static String inputPath;
    private static int workerId;
    private static int workerNum;
    private static GraphDataBufferManager proxy;
    private static Field vertexIdField;
    private static Field vertexValueField;
    private static Field vertexEdgesField;

    static {
        try {
            vertexIdField = VertexImpl.class.getDeclaredField("initializeOid");
            vertexIdField.setAccessible(true);
            vertexValueField = VertexImpl.class.getDeclaredField("initializeVdata");
            vertexValueField.setAccessible(true);
            vertexEdgesField = VertexImpl.class.getDeclaredField("initializeEdges");
            vertexEdgesField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    public FileLoader() {

    }

    public static void init(int workerId, int workerNum, int threadNum, FFIByteVecVector vidBuffers, FFIByteVecVector vertexDataBuffers,
        FFIByteVecVector edgeSrcIdBuffers, FFIByteVecVector edgeDstIdBuffers,
        FFIByteVecVector edgeDataBuffers,
        FFIIntVecVector vidOffsets,
        FFIIntVecVector vertexDataOffsets,
        FFIIntVecVector edgeSrcIdOffsets,
        FFIIntVecVector edgeDstIdOffsets,
        FFIIntVecVector edgeDataOffsets) {
        FileLoader.workerId = workerId;
        FileLoader.workerNum = workerNum;
        FileLoader.threadNum = threadNum;
        FileLoader.executor = Executors.newFixedThreadPool(threadNum);
        //Create a proxy form adding vertex and adding edges
        proxy = new GraphDataBufferManangerImpl(workerId, threadNum, vidBuffers,
            vertexDataBuffers, edgeSrcIdBuffers, edgeDstIdBuffers
            , edgeDataBuffers, vidOffsets, vertexDataOffsets, edgeSrcIdOffsets, edgeDstIdOffsets,
            edgeDataOffsets);
    }

    /**
     *
     * @param inputPath
     * @param vertexInputFormatClass
     * @param params the json params contains giraph configuration.
     */
    public static void loadVerticesAndEdges(String inputPath,
        String vertexInputFormatClass, String params){
//        FileLoader.inputPath = inputPath;
        //Vertex input format class has already been verified, just load.
        Configuration configuration = new Configuration();
        GiraphConfiguration giraphConfiguration = new GiraphConfiguration(configuration);

        try {
            ConfigurationUtils.parseArgs(giraphConfiguration, JSONObject.parseObject(params));
            //            ConfigurationUtils.parseJavaFragment(giraphConfiguration, fragment);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        ImmutableClassesGiraphConfiguration conf = new ImmutableClassesGiraphConfiguration(giraphConfiguration);
        try {
            inputFormatClz = (Class<? extends VertexInputFormat>) Class
                .forName(vertexInputFormatClass);
            vertexInputFormat = inputFormatClz.newInstance();
            vertexInputFormat.setConf(conf);
            Method loadClassLoaderMethod =
                inputFormatClz.getDeclaredMethod("createVertexReader", InputSplit.class,
                    TaskAttemptContext.class);
            InputSplit inputSplit = new InputSplit() {
                @Override
                public long getLength() throws IOException, InterruptedException {
                    return 0;
                }

                @Override
                public String[] getLocations() throws IOException, InterruptedException {
                    return new String[0];
                }
            };
            TaskAttemptID taskAttemptID = new TaskAttemptID();
            TaskAttemptContext taskAttemptContext = new TaskAttemptContext(configuration,
                taskAttemptID);
            vertexReader = (VertexReader) loadClassLoaderMethod
                .invoke(vertexInputFormat, inputSplit, taskAttemptID);
            logger.info("vertex reader: " + vertexReader.getClass().toString());
            vertexReaderClz = vertexReader.getClass();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        loadVertices(inputPath);
    }

    public static void loadVertices(String inputPath) {
        //Try to get number of lines
        long numOfLines = getNumLinesOfFile(inputPath);
        long linesPerWorker = (numOfLines + (workerNum - 1)) / workerNum;
        long start = Math.min(linesPerWorker * workerId, numOfLines);
        long end = Math.min(linesPerWorker * (workerId + 1), numOfLines);
        long chunkSize = (end - start + threadNum - 1) / threadNum;
        logger.debug("total lines {}, worker {} read {}, thread num {}, chunkSize {}", numOfLines,
            workerId, end - start, threadNum, chunkSize);
        long cur = start;

        Future[] futures = new Future[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            LoaderCallable loaderCallable = new LoaderCallable(i, inputPath, Math.min(cur, end),
                Math.min(cur + chunkSize, end));
            futures[i] = executor.submit(loaderCallable);
            cur += chunkSize;
        }

        try {
            long sum = 0;
            for (int i = 0; i < threadNum; ++i) {
                sum += (int) futures[i].get();
            }
            logger.info("worker {} loaded {} lines ", workerId, sum);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return;
        }
    }

    @Override
    public LoaderBase.TYPE loaderType() {
        return TYPE.FileLoader;
    }

    @Override
    public int concurrency() {
        return threadNum;
    }

    static class LoaderCallable implements Callable<Long> {

        private int threadId;
        private BufferedReader bufferedReader;
        private long start;
        private long end; //exclusive

        public LoaderCallable(int threadId, String inputPath, long startLine, long endLine) {
            try {
                FileReader fileReader = new FileReader(inputPath);
                bufferedReader = new BufferedReader(fileReader);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            this.threadId = threadId;
            this.start = startLine;
            this.end = endLine;
            proxy.reserveNumVertices((int) this.end - (int) this.start);
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Long call() throws Exception {
            long cnt = 0;
            while (cnt < start) {
                bufferedReader.readLine();
            }
            while (cnt < end) {
                String line = bufferedReader.readLine();
                Text text = new Text(line);

                if (vertexReader.nextVertex()) {
                    Vertex vertex = vertexReader.getCurrentVertex();
                    Writable vertexId = (Writable) vertexIdField.get(vertex);
                    Writable vertexValue = (Writable) vertexValueField.get(vertex);
                    Iterable<Edge> vertexEdges = (Iterable<Edge>) vertexEdgesField.get(vertex);
                    logger.debug("id {} value {} edges {}", vertexId, vertexValue, vertexEdges);
                    proxy.addVertex(threadId, vertexId, vertexValue);
                    //suppose directed.
                    proxy.addEdges(threadId, vertexId, vertexEdges);
                }
            }
            return cnt;
        }
    }
}
