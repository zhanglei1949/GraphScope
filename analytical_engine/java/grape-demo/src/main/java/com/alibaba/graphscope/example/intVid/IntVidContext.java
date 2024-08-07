package com.alibaba.graphscope.example.intVid;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IntVidContext extends VertexDataContext<IFragment<Integer, Integer, Double, Long>, Long>
        implements ParallelContextBase<Integer, Integer, Double, Long> {
    public class PathStorage extends FileObjectStorage{
        //这里我们使用一个List<Long>来存储一个点的path
        private List<List<Long>> dummy;
        private long beginVertex;
        private long endVertex;
        public PathStorage(String path, long beginVertex, long endVertex){
            super(path);
            this.dummy = new ArrayList<>();
            for (long i = beginVertex; i <= endVertex; ++i){
                //每个点的path都是一个空的List
                dummy.add(new ArrayList<>());
            }
        }

        public long getBeginVertex(){
            return beginVertex;
        }

        public long getEndVertex(){
            return endVertex;
        }

        public List<Long> getPath(long vid){
            return dummy.get((int)(vid - beginVertex));
        }

        @Override
        public void clearInMemory() {
            for (List<Long> l : dummy){
                l.clear();
            }
        }

        @Override
        public void loadObjects(ObjectInputStream in) {
            clearInMemory();
            try {
                int length = in.readInt();
                for (int i = 0; i < length; ++i){
                    int size = in.readInt();
                    List<Long> l = new ArrayList<>();
                    for (int j = 0; j < size; ++j){
                        l.add(in.readLong());
                    }
                    dummy.add(l);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void dumpObjects(ObjectOutputStream out) {
            try {
                out.writeInt(dummy.size());
                for (List<Long> l : dummy){
                    out.writeInt(l.size());
                    for (long i : l){
                        out.writeLong(i);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            clearInMemory();
        }
    }
    public class Msg{
        public Msg(){

        }
    }
    private static Logger logger = LoggerFactory.getLogger(IntVidContext.class);
    private int batchNum = 3; // split the superstep into 3 mini-supersteps
    private int batchNumToModule = 4;
    public int maxSuperStep = 5;
    private int curSuperStep = 0;
    public int threadNum = 1;
    public ExecutorService executor;

    private List<PathStorage> pathStorages; // 长度为batchNum
    public List<List<Msg>> tmpMessageStore; // 保存在每个Batch里收到的消息，并且在最后一个batch中进行处理，清空。

    public List<PathStorage> getPathStorages(){
        return pathStorages;
    }
    /**
     * Called by grape framework, before any PEval. You can initiating data structures need during
     * super steps here.
     *
     * @param frag           The graph fragment providing accesses to graph data.
     * @param messageManager The message manger which manages messages between fragments.
     * @param jsonObject     String args from cmdline.
     * @see IFragment
     * @see ParallelMessageManager
     * @see JSONObject
     */
    @Override
    public void Init(IFragment<Integer, Integer, Double, Long> frag, ParallelMessageManager messageManager, JSONObject jsonObject) {
        createFFIContext(frag, Long.class, false);
        logger.info("Initiating IntVidContext");
        if (jsonObject.containsKey("batchNum")) {
            batchNum = jsonObject.getInteger("batchNum");
        }
        if (jsonObject.containsKey("maxSuperStep")){
            maxSuperStep = jsonObject.getInteger("maxSuperStep");
        }
        if (jsonObject.containsKey("threadNum")){
            threadNum = jsonObject.getInteger("threadNum");
        }
        logger.info("miniSuperStep: " + batchNum);
        batchNumToModule = batchNum + 1; // In the last super step, we iterate over all received messages, and update the value of the vertex.
        pathStorages = new ArrayList<>();
        long verticesPerBatch = (frag.getInnerVerticesNum() + 1) / batchNum;
        for (int i = 0; i < batchNum; ++i){
            long beingVertex = i * verticesPerBatch;
            long endVertex = Math.min((i + 1) * verticesPerBatch, frag.getInnerVerticesNum());
            // [beginVertex, endVertex)
            pathStorages.add(new PathStorage(getPath(frag.fid(), beingVertex, endVertex), beingVertex, endVertex));
        }
        executor = Executors.newFixedThreadPool(threadNum);
    }

    /**
     * Output will be executed when the computations finalizes. Data maintained in this context
     * shall be outputted here.
     *
     * @param frag The graph fragment contains the graph info.
     * @see IFragment
     */
    @Override
    public void Output(IFragment<Integer, Integer, Double, Long> frag) {

    }

    //返回在BFS模型下真正的超步数
    public int getBSPCurSuperStep(){
        return curSuperStep / batchNumToModule;
    }

    // 返回当前超步中的batchId
    public int getCurBatchId() {
        return curSuperStep % batchNumToModule;
    }

    // 返回每个超步中的batch size
    public int getBatchNum() {
        return batchNum;
    }

    public void incCurSuperStep(){
        curSuperStep++;
    }

    private String getPath(int fid, long beginVertex, long endVertex){
        return "path_storage_" + fid + "_" + beginVertex + "_" + endVertex;
    }


}
