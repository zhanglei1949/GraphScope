package com.alibaba.grapscope.example.stringApp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.IntArrayWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BfsContext extends VertexDataContext<IFragment<StringView, Long, Long, Long>, Long> implements ParallelContextBase<StringView, Long, Long, Long> {

    private static Logger logger = LoggerFactory.getLogger(BfsContext.class);

    // bfs起点的原始id
    public String sourceOid;
    //用来保存中间结果
    public IntArrayWrapper partialResults;
    // 用来保存点激活信息的bitset
    public VertexSet currentInnerUpdated, nextInnerUpdated;
    //当前迭代深度
    public int currentDepth;
    // 单个worker使用的线程数量
    public int threadNum;
    // 线程池
    public ExecutorService executor;

    @Override
    public void Init(IFragment<StringView, Long, Long, Long> iFragment,  //代表当前的图分区
        ParallelMessageManager parallelMessageManager,
        JSONObject jsonObject) { //所有的用户参数通过json对象传入
        //必须调用这一句来初始化对应的c++ Context
        createFFIContext(iFragment, Long.class, false);
        if (!jsonObject.containsKey("src")) {
            logger.error("No src arg found");
            return;
        }
        sourceOid = jsonObject.getString("src");
        if (!jsonObject.containsKey("threadNum")) {
            logger.warn("No threadNum arg found");
            threadNum = 1;
        } else {
            threadNum = jsonObject.getInteger("threadNum");
        }
        partialResults = new IntArrayWrapper(iFragment.getVerticesNum().intValue(), Integer.MAX_VALUE);
        currentInnerUpdated = new VertexSet(iFragment.innerVertices());
        nextInnerUpdated = new VertexSet(iFragment.innerVertices());
        currentDepth = 0;
        executor = Executors.newFixedThreadPool(threadNum);
        parallelMessageManager.initChannels(threadNum);
    }

    @Override
    public void Output(IFragment<StringView, Long, Long, Long> iFragment) {
        logger.info("depth: " + currentDepth);
        //c++context提供的VertexArray
        GSVertexArray<Long> vertexArray = data();
        for (Vertex<Long> vertex : iFragment.innerVertices().longIterable()) {
            vertexArray.setValue(vertex, (long) partialResults.get(vertex));
        }
        //用户将需要保存的数据输出到vertexArray中，GRAPE JDK框架会负责将数据写入到odps table中
        logger.info("Finish writing back msg to context");
    }
}
