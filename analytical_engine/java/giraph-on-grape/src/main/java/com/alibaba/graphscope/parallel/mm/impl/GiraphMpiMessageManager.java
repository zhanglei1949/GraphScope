package com.alibaba.graphscope.parallel.mm.impl;

import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.mm.ListMessageIterable;
import com.alibaba.graphscope.parallel.mm.MessageIterable;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class GiraphMpiMessageManager<
    OID_T extends WritableComparable,
    VDATA_T extends Writable,
    EDATA_T extends Writable,
    IN_MSG_T extends Writable,
    OUT_MSG_T extends Writable, GS_VID_T,GS_OID_T>
    implements GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T,GS_OID_T> {
    private static Logger logger = LoggerFactory.getLogger(GiraphDefaultMessageManager.class);

    private ImmutableClassesGiraphConfiguration configuration;
    /** If cached message exceeds this threshold, we will send them immediately. */
    public static long THRESHOLD = 1024 * 1024;

    private SimpleFragment<GS_OID_T,GS_VID_T,?,?> fragment;
    private DefaultMessageManager grapeMessageManager;
    private com.alibaba.graphscope.ds.Vertex<GS_VID_T> grapeVertex;
    private VertexRange innerVertices;
    private long maxInnerVertexLid;
    private int fragmentNum;
    private int fragId;
    private MessageIterable<IN_MSG_T>[] receivedMessages;

    private FFIByteVectorInputStream messagesIn;
    private FFIByteVectorOutputStream[] messagesOut;

    public GiraphMpiMessageManager(
        SimpleFragment fragment,
        DefaultMessageManager defaultMessageManager,
        ImmutableClassesGiraphConfiguration configuration) {
        this.fragment = fragment;
        this.fragmentNum = fragment.fnum();
        this.fragId = fragment.fid();
        this.innerVertices = fragment.innerVertices();
        this.maxInnerVertexLid = (long) this.innerVertices.end().GetValue();

        this.grapeMessageManager = defaultMessageManager;
        this.grapeVertex = FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
        this.messagesIn = new FFIByteVectorInputStream();
        //        this.messagesOutToSelf = new FFIByteVectorOutputStream();
        this.messagesOut = new FFIByteVectorOutputStream[fragment.fnum()];
        for (int i = 0; i < fragment.fnum(); ++i) {
            this.messagesOut[i] = new FFIByteVectorOutputStream();
            this.messagesOut[i].resize(THRESHOLD);
        }

        this.receivedMessages = new MessageIterable[(int) fragment.getInnerVerticesNum()];
        for (int i = 0; i < fragment.getInnerVerticesNum(); ++i) {
            this.receivedMessages[i] = new ListMessageIterable<>();
        }

        this.configuration = configuration;
    }

    /**
     * Called by our frame work, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {
        // Clear the message receiving buffers.
        parallelClearReceiveMessages();

        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        while (grapeMessageManager.getPureMessage(tmpVector)) {
            // OutArchive will do the resize;
            logger.info(
                "Frag ["
                    + fragId
                    + "]  digest message: "
                    + tmpVector.getAddress()
                    + ", msg size:"
                    + tmpVector.size());
            this.messagesIn.digestVector(tmpVector);
        }
        // Parse messageIn and form into Iterable<message> for each vertex;
        logger.info(
            "Frag ["
                + fragId
                + "] totally Received ["
                + messagesIn.longAvailable()
                + "] bytes, starting deserialization");
        if (configuration.getGrapeVidClass().equals(Long.class)) {
            com.alibaba.graphscope.ds.Vertex<Long> longVertex =
                (com.alibaba.graphscope.ds.Vertex<Long>) grapeVertex;
        } else {
            throw new IllegalStateException("Expect long vid");
        }

        try {
            while (true) {
                if (messagesIn.available() <= 0) {
                    break;
                }
                long dstVertexGid = messagesIn.readLong();

                //                Writable inMsg = WritableFactory.newInMsg();
                Writable inMsg = configuration.createInComingMessageValue();
                inMsg.readFields(messagesIn);

                // store the msg
                fragment.gid2Vertex((GS_VID_T) (Long)dstVertexGid, grapeVertex);
                com.alibaba.graphscope.ds.Vertex<Long> grapeVertex2 =
                    (com.alibaba.graphscope.ds.Vertex<Long>) grapeVertex;
                if (grapeVertex2.GetValue() >= maxInnerVertexLid) {
                    logger.error("Received one vertex id which exceeds inner vertex range.");
                    return;
                }
                receivedMessages[grapeVertex2.GetValue().intValue()].append((IN_MSG_T) inMsg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        if (lid >= maxInnerVertexLid) {
            logger.error("max lid: " + maxInnerVertexLid + ", " + lid + " execeds.");
            return null;
        }
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
        if (lid >= maxInnerVertexLid) {
            logger.error("max lid: " + maxInnerVertexLid + ", " + lid + " execeds.");
            return false;
        }
        return receivedMessages[(int) lid].size() > 0;
    }

    /**
     * Send one message to dstOid.
     *
     * @param dstOid vertex to receive this message.
     * @param message message.
     */
    @Override
    public void sendMessage(OID_T dstOid, OUT_MSG_T message) {
        if (dstOid instanceof LongWritable) {
            LongWritable longOid = (LongWritable) dstOid;
            // Get lid from oid
            boolean res = fragment.getVertex((GS_OID_T) (Long)longOid.get(), grapeVertex);
            //            tmpVertex.SetValue(longOid.get());
            logger.debug("oid -> lid: return :" + res + ", oid:" + longOid + ", " + grapeVertex.GetValue());
            int dstfragId = fragment.getFragId(grapeVertex);
            if (dstfragId != fragId && messagesOut[dstfragId].bytesWriten() >= THRESHOLD) {
                messagesOut[dstfragId].writeLong(0, messagesOut[dstfragId].bytesWriten() - 8); // minus size_of_int
                messagesOut[dstfragId].finishSetting();
                grapeMessageManager.sendToFragment(dstfragId, messagesOut[dstfragId].getVector());
//                messagesOut[dstfragId].reset();
                messagesOut[dstfragId] = new FFIByteVectorOutputStream();
                messagesOut[dstfragId].resize(THRESHOLD);
                messagesOut[dstfragId].writeLong(0, 0);
            }
            try {
                messagesOut[dstfragId].writeLong((Long) fragment.vertex2Gid(grapeVertex));
                message.write(messagesOut[dstfragId]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.error("Expect a longWritable oid");
        }
    }

    /**
     * Send message to neighbor vertices.
     *
     * @param vertex
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {
        VertexImpl<OID_T, VDATA_T, EDATA_T> vertexImpl =
            (VertexImpl<OID_T, VDATA_T, EDATA_T>) vertex;
        grapeVertex.SetValue((GS_VID_T) (Long)vertexImpl.getLocalId());

        // send msg through outgoing adjlist
        AdjList adjList = fragment.getOutgoingAdjList(grapeVertex);
        Iterable<Nbr> iterable = adjList.iterable();
        com.alibaba.graphscope.ds.Vertex<GS_VID_T> curVertex;
        try {
            for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
                Nbr nbr = it.next();
                curVertex = nbr.neighbor();
                int dstfragId = fragment.getFragId(curVertex);
                if (dstfragId != fragId && messagesOut[dstfragId].bytesWriten() >= THRESHOLD) {
                    messagesOut[dstfragId].writeLong(0, messagesOut[dstfragId].bytesWriten() - 8); // minus size_of_int
                    messagesOut[dstfragId].finishSetting();
                    grapeMessageManager.sendToFragment(dstfragId, messagesOut[dstfragId].getVector());
//                messagesOut[dstfragId].reset();
                    messagesOut[dstfragId] = new FFIByteVectorOutputStream();
                    messagesOut[dstfragId].resize(THRESHOLD);
                    messagesOut[dstfragId].writeLong(0, 0);
                }
                messagesOut[dstfragId].writeLong((Long) fragment.vertex2Gid(curVertex));
                message.write(messagesOut[dstfragId]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // send msg through incoming adjlist
        adjList = fragment.getIncomingAdjList(grapeVertex);
        iterable = adjList.iterable();
        try {
            for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
                Nbr nbr = it.next();
                curVertex = nbr.neighbor();

                int dstfragId = fragment.getFragId(curVertex);
                if (dstfragId != fragId && messagesOut[dstfragId].bytesWriten() >= THRESHOLD) {
                    messagesOut[dstfragId].writeLong(0, messagesOut[dstfragId].bytesWriten() - 8); // minus size_of_int
                    messagesOut[dstfragId].finishSetting();
                    grapeMessageManager.sendToFragment(dstfragId, messagesOut[dstfragId].getVector());
//                messagesOut[dstfragId].reset();
                    messagesOut[dstfragId] = new FFIByteVectorOutputStream();
                    messagesOut[dstfragId].resize(THRESHOLD);
                    messagesOut[dstfragId].writeLong(0, 0);
                }
                messagesOut[dstfragId].writeLong((Long) fragment.vertex2Gid(curVertex));
                message.write(messagesOut[dstfragId]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        logger.debug(
//                "After send messages from vertex: "
//                        + grapeVertex.GetValue()
//                        + " through all edges");
//        for (int i = 0; i < fragment.fnum(); ++i) {
//            logger.debug("To frag[" + i + "]: " + messagesOut[i].bytesWriten());
//        }
    }

    /** Make sure all messages has been sent. Clean outputstream buffer */
    @Override
    public void finishMessageSending() {
        this.messagesIn.clear();
        for (int i = 0; i < fragmentNum; ++i) {
            long size = messagesOut[i].bytesWriten();
            if (i != fragId){
                logger.info("finish msg sending: " + fragId + "->" + i + ": " + (messagesOut[i].bytesWriten() - 8));
                messagesOut[i].writeLong(0, messagesOut[i].bytesWriten() - 8); // minus size_of_long
            }
            messagesOut[i].finishSetting();

            if (size == 8) { // size be at least 8.
                logger.info(
                    "In final step,Message from frag["
                        + fragId
                        + "] to frag ["
                        + i
                        + "] empty.");
                continue;
            }

            if (i != fragId) {
                FFIByteVector vector =messagesOut[i].getVector();
//                logger.info(" vector size: " + vector.size() + "size: " + size);
//                for (int j = 0; j < size; ++j){
//                    logger.info("index: [" + j + "]: " + vector.getRaw(j));
//                }
                grapeMessageManager.sendToFragment(i, messagesOut[i].getVector());
                logger.info(
                    "In final step, Frag ["
                        + fragId
                        + "] sending to frag ["
                        + i
                        + "] msg of size: "
                        + size);
            } else {
                // For messages send to local, we just do digest.
                messagesIn.digestVector(messagesOut[i].getVector());
                logger.info(
                    "In final step, Frag [" + fragId + "] digest msg to self of size: " + size);
            }
//            messagesOut[i].reset();
            messagesOut[i] = new FFIByteVectorOutputStream();
            messagesOut[i].resize(THRESHOLD);
        }
    }

    /**
     * Check any messages to self.
     *
     * @return true if messages sent to self.
     */
    @Override
    public boolean anyMessageReceived() {
        return messagesIn.longAvailable() > 0;
    }

    /**
     * @param curVertex
     * @return
     */
    private boolean isInnerVertex(com.alibaba.graphscope.ds.Vertex<Long> curVertex) {
        return curVertex.GetValue().intValue() < maxInnerVertexLid;
    }

    /** Clear the messageIterables in parallel. */
    private void parallelClearReceiveMessages() {
        for (int i = 0; i < maxInnerVertexLid; ++i) {
            receivedMessages[i].clear();
        }
    }

    @Override
    public void forceContinue() {
        grapeMessageManager.ForceContinue();
    }

    @Override
    public void preSuperstep() {
        //reset messagesout here, so that the data buffer can be safely digested by
        //grape message manager. Don't reset them after finishMessageSetting.
        for (int i = 0; i < fragmentNum; ++i){
//            messagesOut[i].reset();
//            messagesOut[i] = new FFIByteVectorOutputStream();
//            messagesOut[i].resize(THRESHOLD);
            if (i != fragId){
                //only write size info for mpi messages, local message don't need size.
                try {
                    messagesOut[i].writeLong(0); // a place holder which hold then length of this vector. will be set to actual length when writing is over.
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void postSuperstep() {

    }

    @Override
    public void postApplication(){

    }
}
