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
    public static long THRESHOLD = 1024 * 16;

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
        for (int i = 0; i < fragment.getInnerVerticesNum(); ++i){
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
        for (int i = 0; i < fragmentNum; ++i) {
            if (i == fragId) {
                continue;
            }
            FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
            grapeMessageManager.getPureMessage(tmpVector);
            this.messagesIn.digestVector(tmpVector);
        }
        //Parse messageIn and form into Iterable<message> for each vertex;
        logger.info("Received [" + messagesIn.longAvailable() + "] bytes, starting deserialization");

        com.alibaba.graphscope.ds.Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        try {
            while (true) {
                if (messagesIn.available() <= 0) {
                    break;
                }
                vertex.SetValue(messagesIn.readLong());
                Writable inMsg = WritableFactory.newInMsg();
                inMsg.readFields(messagesIn);
                //TODO: only for testing
                LongWritable inMsg2 = (LongWritable) inMsg;
                logger.info("read vertex: " + vertex.GetValue() + "msg: " + inMsg2.get());

                //store the msg
                fragment.gid2Vertex(vertex.GetValue(), vertex);
                if (vertex.GetValue() >= maxInnerVertexLid){
                    logger.error("Received one vertex id which exceeds inner vertex range.");
                    return ;
                }
                receivedMessages[vertex.GetValue().intValue()].append((IN_MSG_T) inMsg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        return receivedMessages[(int)lid];
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
                        .sendToFragment(fragment, messagesOut[dstfragId].getVector());
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
                        .sendToFragment(fragment, messagesOut[dstfragId].getVector());
                    messagesOut[dstfragId].reset();
                }
                messagesOut[dstfragId].writeLong((Long) fragment.vertex2Gid(curVertex));
                message.write(messagesOut[dstfragId]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("After processing msg from vertex: " + grapeVertex.GetValue());
        for (int i = 0; i < fragment.fnum(); ++i) {
            logger.info("To frag[i]: " + messagesOut[i].bytesWriten());
        }
    }

    /**
     * Make sure all messages has been sent.
     */
    @Override
    public void finishMessageSending() {
        for (int i = 0; i < fragmentNum; ++i) {
            long size = messagesOut[i].bytesWriten();
            messagesOut[i].finishSetting();
            if (i != fragId) {
                //TODO: make sure the vector is tight, before sending to fragment. i.e. size = offset
                grapeMessageManager.sendToFragment(fragment, messagesOut[i].getVector());
                logger.info(
                    "In final step, Frag [" + fragId + "] sending to frag [" + i + "] msg of size: "
                        + size);
            } else {
                //For messages send to local, we just do digest.
                messagesIn.digestVector(messagesOut[i].getVector());
                logger.info(
                    "In final step, Frag [" + fragId + "] digest msg to self of size: " + size);
            }
        }
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
    private void parallelClearReceiveMessages(){
        for (int i = 0; i < maxInnerVertexLid; ++i){
            receivedMessages[i].clear();
        }
    }
}
