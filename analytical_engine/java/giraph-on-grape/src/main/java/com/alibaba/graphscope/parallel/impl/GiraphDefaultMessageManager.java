package com.alibaba.graphscope.parallel.impl;

import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.MessageIterable;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.WritableFactory;
import java.io.IOException;
import java.util.Iterator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GiraphDefaultMessageManager<OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable, IN_MSG_T extends Writable, OUT_MSG_T extends Writable> implements
    GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T> {

    /**
     * If cached message exceeds this threshold, we will send them immediately.
     */
    public static long THRESHOLD = 1024 * 512;

    private static Logger logger = LoggerFactory.getLogger(GiraphDefaultMessageManager.class);

    private SimpleFragment fragment;
    private DefaultMessageManager grapeMessageManager;
    private com.alibaba.graphscope.ds.Vertex<Long> grapeVertex;
    private VertexRange<Long> innerVertices;
    private long maxInnerVertexLid;
    private int fragmentNum;
    private int fragId;
    private MessageIterable<IN_MSG_T>[] receivedMessages;

    private FFIByteVectorInputStream messagesIn;
    private FFIByteVectorOutputStream[] messagesOut;


    public GiraphDefaultMessageManager(SimpleFragment fragment,
        DefaultMessageManager defaultMessageManager) {
        this.fragment = fragment;
        this.fragmentNum = fragment.fnum();
        this.fragId = fragment.fid();
        this.innerVertices = fragment.innerVertices();
        this.maxInnerVertexLid = this.innerVertices.end().GetValue();

        this.grapeMessageManager = defaultMessageManager;
        this.grapeVertex = FFITypeFactoryhelper.newVertexLong();
        this.messagesIn = new FFIByteVectorInputStream();
//        this.messagesOutToSelf = new FFIByteVectorOutputStream();
        this.messagesOut = new FFIByteVectorOutputStream[fragment.fnum()];
        for (int i = 0; i < fragment.fnum(); ++i) {
            this.messagesOut[i] = new FFIByteVectorOutputStream();
        }

        this.receivedMessages = new MessageIterable[(int) fragment.getInnerVerticesNum()];
        for (int i = 0; i < fragment.getInnerVerticesNum(); ++i) {
            this.receivedMessages[i] = new MessageIterable<>();
        }
    }

    /**
     * Called by our frame work, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {
        //Clear the message receiving buffers.
        parallelClearReceiveMessages();

        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        while (grapeMessageManager.getPureMessage(tmpVector)) {
            //OutArchive will do the resize;
            logger.info("Frag [" + fragId + "before digest: " + tmpVector.getAddress());
            this.messagesIn.digestVector(tmpVector);
            logger.info("Frag [" + fragId + "after digest: " + tmpVector.getAddress());
        }
        //Parse messageIn and form into Iterable<message> for each vertex;
        logger.info("Frag [" + fragId + "] totally Received [" + messagesIn.longAvailable()
            + "] bytes, starting deserialization");

        com.alibaba.graphscope.ds.Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        try {
            while (true) {
                if (messagesIn.available() <= 0) {
                    break;
                }
                long dstVertexGid = messagesIn.readLong();

                Writable inMsg = WritableFactory.newInMsg();
                inMsg.readFields(messagesIn);
                //TODO: only for testing
                LongWritable inMsg2 = (LongWritable) inMsg;
                logger.debug("Got message to vertex, gid" + dstVertexGid + "msg: " + inMsg2.get());

                //store the msg
                fragment.gid2Vertex(dstVertexGid, vertex);
                if (vertex.GetValue() >= maxInnerVertexLid) {
                    logger.error("Received one vertex id which exceeds inner vertex range.");
                    return;
                }
                receivedMessages[vertex.GetValue().intValue()].append((IN_MSG_T) inMsg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        return receivedMessages[(int) lid];
    }

    /**
     * Check any message available on this vertex.
     *
     * @param lid local id
     * @return true if recevied messages.
     */
    @Override
    public boolean messageAvailable(long lid) {
        return receivedMessages[(int) lid].size() > 0;
    }

    /**
     * Send message to neighbor vertices.
     *
     * @param vertex
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {
        VertexImpl<OID_T, VDATA_T, EDATA_T> vertexImpl = (VertexImpl<OID_T, VDATA_T, EDATA_T>) vertex;
        grapeVertex.SetValue(vertexImpl.getLocalId());

        //send msg through outgoing adjlist
        AdjList adjList = fragment.getOutgoingAdjList(grapeVertex);
        Iterable<Nbr> iterable = adjList.iterator();
        com.alibaba.graphscope.ds.Vertex<Long> curVertex;
        try {
            for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
                Nbr nbr = it.next();
                curVertex = nbr.neighbor();
                int dstfragId = fragment.getFragId(curVertex);
                if (dstfragId != fragId && messagesOut[dstfragId].bytesWriten() >= THRESHOLD) {
                    messagesOut[dstfragId].finishSetting();
                    grapeMessageManager
                        .sendToFragment(dstfragId, messagesOut[dstfragId].getVector());
                    messagesOut[dstfragId].reset();
                }
                messagesOut[dstfragId].writeLong((Long) fragment.vertex2Gid(curVertex));
                message.write(messagesOut[dstfragId]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //send msg through incoming adjlist
        adjList = fragment.getIncomingAdjList(grapeVertex);
        iterable = adjList.iterator();
        try {
            for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
                Nbr nbr = it.next();
                curVertex = nbr.neighbor();

                int dstfragId = fragment.getFragId(curVertex);
                if (dstfragId != fragId && messagesOut[dstfragId].bytesWriten() >= THRESHOLD) {
                    messagesOut[dstfragId].finishSetting();
                    grapeMessageManager
                        .sendToFragment(dstfragId, messagesOut[dstfragId].getVector());
                    messagesOut[dstfragId].reset();
                }
                messagesOut[dstfragId].writeLong((Long) fragment.vertex2Gid(curVertex));
                message.write(messagesOut[dstfragId]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug(
            "After send messages from vertex: " + grapeVertex.GetValue() + " through all edges");
        for (int i = 0; i < fragment.fnum(); ++i) {
            logger.debug("To frag[" + i + "]: " + messagesOut[i].bytesWriten());
        }
    }

    /**
     * Make sure all messages has been sent. Clean outputstream buffer
     */
    @Override
    public void finishMessageSending() {
        this.messagesIn.clear();
        for (int i = 0; i < fragmentNum; ++i) {
            long size = messagesOut[i].bytesWriten();
            messagesOut[i].finishSetting();

            if (size == 0) {
                logger.info("In final step,Message from frag[" + fragId + "] to frag [" + i + "] empty.");
                continue;
            }

            if (i != fragId) {
                grapeMessageManager.sendToFragment(i, messagesOut[i].getVector());
                logger.info(
                    "In final step, Frag [" + fragId + "] sending to frag [" + i + "] msg of size: "
                        + size);
            } else {
                //For messages send to local, we just do digest.
                messagesIn.digestVector(messagesOut[i].getVector());
                logger.info(
                    "In final step, Frag [" + fragId + "] digest msg to self of size: " + size);
            }
            messagesOut[i].reset();
        }
    }

    /**
     * Check any messages to self.
     *
     * @return true if messages sent to self.
     */
    @Override
    public boolean anyMessageToSelf() {
        return messagesIn.longAvailable() > 0;
    }

    /**
     * @param curVertex
     * @return
     */
    private boolean isInnerVertex(com.alibaba.graphscope.ds.Vertex<Long> curVertex) {
        return curVertex.GetValue().intValue() < maxInnerVertexLid;
    }

    /**
     * Clear the messageIterables in parallel.
     */
    private void parallelClearReceiveMessages() {
        for (int i = 0; i < maxInnerVertexLid; ++i) {
            receivedMessages[i].clear();
        }
    }
}
