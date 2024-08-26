package com.alibaba.graphscope.example.intVid;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.parallel.message.MsgBase;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class IntVid implements ParallelAppBase<Integer, Integer, Double, Long, IntVidContext> {
    private static Logger logger = LoggerFactory.getLogger(IntVid.class);

    /**
     * Partial Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void PEval(IFragment<Integer, Integer, Double, Long> graph, ParallelContextBase<Integer, Integer, Double, Long> context, ParallelMessageManager messageManager) {
        logger.info("IntVidTest PEval");
        // 我们跳过PEVal，直接进入IncEval。
        // 如果有需要初始化的，不想放在Context里的，也可以放这
        messageManager.forceContinue();
    }

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see ParallelContextBase
     * @see ParallelMessageManager
     */
    @Override
    public void IncEval(IFragment<Integer, Integer, Double, Long> graph, ParallelContextBase<Integer, Integer, Double, Long> context, ParallelMessageManager messageManager) {
        logger.info("IntVidTest IncEval");

        IntVidContext intVidContext = (IntVidContext) context;
        if (intVidContext.getSuperStep() >= intVidContext.maxSuperStep) {
            logger.info("Max Steps reached, return.");
            return;
        }
        for (int batchId = 0; batchId < intVidContext.getBatchNum(); ++batchId) {
            // 我们将在[0, batchNum - 1] 的super step中，分批次发送消息，并且接收消息，并且暂时保存到MessageStorage里。
            processBatchStep(batchId, messageManager, graph, intVidContext);
        }
        //最后读取所有接收到的消息，更新VertexPathStorage
        loadReceivedMessageAndUpdate(intVidContext);
        intVidContext.incCurSuperStep();
    }

    public void loadReceivedMessageAndUpdate(IntVidContext ctx) {
        //将所有接收到的消息，更新到每个vertex的path storage上
        updatePathStorageFromMessage(ctx);

        //清除当前super step的消息
        ctx.clearMessageStorages();
        ctx.clearPathStorages();
    }

    public void processBatchStep(int batchId, ParallelMessageManager messageManager, IFragment<Integer, Integer, Double, Long> frag, IntVidContext ctx) {
        //接收消息，并且暂时存储在PathStorage里
        receiveMessage(messageManager, frag, ctx);
        //发送消息
        sendMessage(messageManager, frag, ctx, batchId);
    }

    //我们暂时假设接收到的消息存储在内存中。如果消息量过多导致内存不足，可以考虑也存储在磁盘上。
    public void receiveMessage(ParallelMessageManager messageManager, IFragment<Integer, Integer, Double, Long> graph, IntVidContext ctx) {
        // 从messageManager中接收消息
        CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
        MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
        int chunkSize = 1024;
        for (int tid = 0; tid < ctx.threadNum; ++tid) {
            final int finalTid = tid;
            ctx.executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            MessageInBuffer messageInBuffer = bufferFactory.create();
                            FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
                            boolean result;
                            while (true) {
                                result = messageManager.getMessageInBuffer(messageInBuffer);
                                if (result) {
                                    try {
                                        receiveMessageImpl(graph, messageManager, ctx, messageInBuffer);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        logger.error(
                                                "Error when receiving message in fragment {} thread {}",
                                                graph.fid(),
                                                finalTid);
                                    }
                                } else {
                                    break;
                                }
                            }
                            countDownLatch.countDown();
                        }
                    });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            ctx.executor.shutdown();
        }
    }

    public void sendMessage(ParallelMessageManager messageManager, IFragment<Integer, Integer, Double, Long> graph, IntVidContext ctx, int batchId) {
        // 从属于batchId中的vertex里，发送消息
        IntVidContext.PathStorage pathStorage = ctx.getPathStorages().get(batchId);
        // load from disk
        pathStorage.load();
        for (long i = pathStorage.getBeginVertex(); i < pathStorage.getEndVertex(); i++) {
            List<Long> path = pathStorage.getPath(i);
            //TODO: send message to neighbors based on this path
        }

        // clear from storage, no need to dump
        pathStorage.clearInMemory();
    }

    public void receiveMessageImpl(IFragment<Integer, Integer, Double, Long> graph, ParallelMessageManager messageManager, IntVidContext ctx, MessageInBuffer messageInBuffer) {
        // 从messageInBuffer中读取消息
        // 更新到ctx.tmpMessageStore中
        //TODO: implement this function
        // 收到的消息有可能去往本frag里面任意一点。
        //替换自己的实现。
        LongMsg msg = FFITypeFactoryhelper.newLongMsg();
        List<List<Long>> messages = new ArrayList<>();
        for (int i = 0; i < ctx.getBatchNum(); ++i) {
            messages.add(new ArrayList<>());
        }
        while (messageInBuffer.getPureMessage(msg)) {
            //获取msg的内容，指向的vertex id，以及消息内容
            //根据指向的veretx_id,获取batch_id，append到messageStore里
            Vertex<Integer> vertex = FFITypeFactoryhelper.newVertexInt();
            vertex.setValue(0);
            long innerVertexId = graph.getInnerVertexId(vertex);
            int batchId = ctx.getBatchIdFromVertexId(innerVertexId);

            //.....
//            messages.get(batchId).add(msg);
        }

        // 将messages存储到ctx里
        for (int i = 0; i < ctx.getBatchNum(); ++i){
            IntVidContext.MessageStorage messageStorage =  ctx.getMessageStorages().get(i);
            messageStorage.load();
            // 将messages[i] 里的数据append  到messageStorage
//            messageStorage.append(vertexInnerVid,  messages.get(i));

            messageStorage.dump();
            messageStorage.clearInMemory();
        }
    }

    public void updatePathStorageFromMessage(IntVidContext ctx) {
        // 分批次，分别读取各个batch的MessageStorage，更新到PathStorage里


    }
}