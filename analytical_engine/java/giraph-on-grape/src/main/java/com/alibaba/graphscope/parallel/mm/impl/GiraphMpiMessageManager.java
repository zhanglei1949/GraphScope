package com.alibaba.graphscope.parallel.mm.impl;

import static org.apache.giraph.conf.GiraphConstants.MAX_OUT_MSG_CACHE_SIZE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;

import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import java.io.IOException;
import java.util.Iterator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GiraphMpiMessageManager<
    OID_T extends WritableComparable,
    VDATA_T extends Writable,
    EDATA_T extends Writable,
    IN_MSG_T extends Writable,
    OUT_MSG_T extends Writable, GS_VID_T, GS_OID_T>
    extends
    AbstractMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T, GS_OID_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphMpiMessageManager.class);

    public static long THRESHOLD;

    private FFIByteVectorOutputStream[] cacheOut;

    public GiraphMpiMessageManager(
        SimpleFragment fragment,
        DefaultMessageManager defaultMessageManager,
        ImmutableClassesGiraphConfiguration configuration, FFICommunicator communicator) {
        super(fragment, defaultMessageManager, configuration, communicator);
        THRESHOLD = MAX_OUT_MSG_CACHE_SIZE.get(configuration);

//        this.messagesIn = new FFIByteVectorInputStream();
        this.cacheOut = new FFIByteVectorOutputStream[fragment.fnum()];
        for (int i = 0; i < fragment.fnum(); ++i) {
            this.cacheOut[i] = new FFIByteVectorOutputStream();
            this.cacheOut[i].resize(THRESHOLD);
        }
    }

    /**
     * Called by our frame work, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {
        //put message to currentIncoming message store
        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        long bytesOfReceivedMsg = 0;
        while (grapeMessager.getPureMessage(tmpVector)) {
            // OutArchive will do the resize;
            if (logger.isDebugEnabled()) {
                logger.debug("Frag [{}] digest message of size {}", fragId, tmpVector.size());
            }
            currentIncomingMessageStore.digest(tmpVector);
            bytesOfReceivedMsg += tmpVector.size();
        }
        logger.debug(
            "Frag [{}] totally Received [{}] bytes, starting deserialization", fragId,
            bytesOfReceivedMsg);
    }


    /**
     * Send one message to dstOid.
     *
     * @param dstOid  vertex to receive this message.
     * @param message message.
     */
    @Override
    public void sendMessage(OID_T dstOid, OUT_MSG_T message) {
        if (dstOid instanceof LongWritable) {
            LongWritable longOid = (LongWritable) dstOid;
            boolean res = fragment.getVertex((GS_OID_T) (Long) longOid.get(), grapeVertex);

            if (!res) {
                throw new IllegalStateException("Failed to get lid from oid" + dstOid);
            }
            logger.debug("oid [{}] -> lid [{}]", longOid, grapeVertex.GetValue());
            sendMessage(grapeVertex, message);
        } else {
            logger.error("Expect a longWritable oid");
        }
    }

    private void sendMessage(com.alibaba.graphscope.ds.Vertex<GS_VID_T> vertex, OUT_MSG_T msg) {
        int dstfragId = fragment.getFragId(vertex);
        if (dstfragId != fragId && cacheOut[dstfragId].bytesWriten() >= THRESHOLD) {
            cacheOut[dstfragId].writeLong(0,
                cacheOut[dstfragId].bytesWriten() - 8); // minus size_of_long
            cacheOut[dstfragId].finishSetting();
            //the vertex will be swapped. so this vector is empty;
            grapeMessager.sendToFragment(dstfragId, cacheOut[dstfragId].getVector());
            cacheOut[dstfragId] = new FFIByteVectorOutputStream();
            cacheOut[dstfragId].resize(THRESHOLD);
            cacheOut[dstfragId].writeLong(0, 0);
        }
        try {
            cacheOut[dstfragId].writeLong((Long) fragment.vertex2Gid(vertex));
            msg.write(cacheOut[dstfragId]);
        } catch (IOException e) {
            e.printStackTrace();
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
        grapeVertex.SetValue((GS_VID_T) (Long) vertexImpl.getLocalId());

        // send msg through outgoing adjlist
        AdjList adjList = fragment.getOutgoingAdjList(grapeVertex);
        Iterable<Nbr> iterable = adjList.iterable();
        com.alibaba.graphscope.ds.Vertex<GS_VID_T> curVertex;

        for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
            curVertex = it.next().neighbor();
            sendMessage(curVertex, message);
        }
    }

    /**
     * Make sure all messages has been sent. Clean outputstream buffer
     */
    @Override
    public void finishMessageSending() {
        for (int i = 0; i < fragNum; ++i) {
            long size = cacheOut[i].bytesWriten();
            cacheOut[i].finishSetting();

            if (i == fragId) {
                if (size == 0) {
                    logger.debug("[Finish msg] sending skip msg to self, since msg size: {}", size);
                    continue;
                }
//                messagesIn.digestVector(cacheOut[i].getVector());
                nextIncomingMessageStore.digest(cacheOut[i].getVector());
                logger.debug(
                    "In final step, Frag [{}] digest msg to self of size: {}", fragId, size);
            } else {
                if (size == SIZE_OF_LONG) {
                    logger.debug("[Finish msg] sending skip msg from {} -> {}, since msg size: {}",
                        fragId, i, size);
                    continue;
                }
                cacheOut[i].writeLong(0, cacheOut[i].bytesWriten() - SIZE_OF_LONG);
                logger.debug("[Finish msg] sending msg from {} -> {}, actual msg size {}",
                    fragId, i, cacheOut[i].bytesWriten() - SIZE_OF_LONG);
                grapeMessager.sendToFragment(i, cacheOut[i].getVector());
            }
        }
    }


    @Override
    public void preSuperstep() {
        for (int i = 0; i < fragNum; ++i) {
            cacheOut[i].resize(THRESHOLD);
            cacheOut[i].reset();
            if (i != fragId) {
                //only write size info for mpi messages, local message don't need size.
                try {
                    cacheOut[i].writeLong(0);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void postSuperstep() {
        currentIncomingMessageStore.swap(nextIncomingMessageStore);
        nextIncomingMessageStore.clearAll();
    }

    @Override
    public void postApplication() {

    }
}
