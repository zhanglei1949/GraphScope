package com.alibaba.graphscope.loader.impl;

import static com.alibaba.graphscope.loader.LoaderUtils.checkFileExist;
import static com.alibaba.graphscope.loader.LoaderUtils.getNumLinesOfFile;

import com.alibaba.graphscope.loader.GraphDataBufferManager;
import com.alibaba.graphscope.loader.LoaderBase;
import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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
    private int threadNum;
    private Class<? extends VertexInputFormat> inputFormatClz;
    private Class<? extends VertexReader> vertexReaderClz;
    private VertexReader vertexReader;
    private ExecutorService executor;
    private String inputPath;

    public FileLoader() {

    }

    public boolean init(int threadNum, String inputPath, String vertexInputFormatClass) {
        this.threadNum = threadNum;
        executor = Executors.newFixedThreadPool(threadNum);
        if (!checkFileExist(inputPath)) {
            logger.error("Input file doesn't not exist.");
            return false;
        }
        this.inputPath = inputPath;
        //Vertex input format class has already been verified, just load.
        try {
            inputFormatClz = (Class<? extends VertexInputFormat>) Class
                .forName(vertexInputFormatClass);
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
            Configuration configuration = new Configuration();
            TaskAttemptID taskAttemptID = new TaskAttemptID();
            TaskAttemptContext taskAttemptContext = new TaskAttemptContext(configuration,
                taskAttemptID);
            vertexReader = (VertexReader) loadClassLoaderMethod
                .invoke(null, inputSplit, taskAttemptID);
            logger.info("vertex reader: " + vertexReader.getClass().toString());
            vertexReaderClz = vertexReader.getClass();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return true;
    }

    public void loadFragment(FFIByteVecVector vidBuffers, FFIIntVecVector vidOffsets,
        FFIByteVecVector vertexDataBuffers, FFIByteVecVector edgeSrcIdBuffers,
        FFIIntVecVector edgeSrcIdOffsets, FFIByteVecVector edgeDstIdBuffers,
        FFIIntVecVector edgeDstIdOffsets, FFIByteVecVector edgeDataBuffers) {

        //Create a proxy form adding vertex and adding edges
        GraphDataBufferManager proxy = new GraphDataBufferManangerImpl(vidBuffers, vidOffsets,
            vertexDataBuffers, edgeSrcIdBuffers,
            edgeSrcIdOffsets, edgeDstIdBuffers, edgeDstIdOffsets, edgeDataBuffers);

        if (vidBuffers.getAddress() <= 0 || vidOffsets.getAddress() <= 0
            || vertexDataBuffers.getAddress() <= 0 || edgeSrcIdBuffers.getAddress() <= 0
            || edgeSrcIdOffsets.getAddress() <= 0
            || edgeDstIdBuffers.getAddress() <= 0 || edgeDstIdOffsets.getAddress() <= 0
            || edgeDataBuffers.getAddress() <= 0) {
            logger.error("Empty buffers");
            return;
        }

        //Try to get number of lines
        long numOfLines = getNumLinesOfFile(inputPath);
        long chunkSize = (numOfLines + threadNum - 1) / threadNum;
        long cur = 0;

        Future[] futures = new Future[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            long start = Math.min(numOfLines - 1, cur);
            long end = Math.min(numOfLines - 1, cur + chunkSize);
            cur += chunkSize;

            LoaderCallable loaderCallable = new LoaderCallable(threadNum, inputPath, start, end);
            futures[i] = executor.submit(loaderCallable);
        }

        try {
            long sum = 0;
            for (int i = 0; i < threadNum; ++i) {
                sum += (int) futures[i].get();
            }
            logger.info("Loaded [" + "] " + sum + " vertices");
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

    class LoaderCallable implements Callable<Long> {

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

                }
            }
            return cnt;
        }
    }
}
