package com.alibaba.graphscope.parallel.mm.impl;

import static org.apache.giraph.conf.GiraphConstants.MAX_CONN_TRY_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.cache.impl.BatchWritableMessageCache;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.message.MessageStoreFactory;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import com.alibaba.graphscope.parallel.netty.NettyServer;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.Iterator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Giraph message manager relies on netty for ipc communication.
 * <p>
 * One netty base message manager has a netty client and a netty server. Netty Client connect to all
 * other netty server, and netty server is connected by other netty clients.
 * </p>
 *
 * <p>
 * Has similar role with WorkerClientRequestProcessor in Giraph.
 * </p>
 *
 * @param <OID_T>     original id
 * @param <VDATA_T>   vertex data
 * @param <EDATA_T>   edge data
 * @param <IN_MSG_T>  incoming msg type
 * @param <OUT_MSG_T> outgoing msg type
 */
public class GiraphNettyMessageManager<
    OID_T extends WritableComparable,
    VDATA_T extends Writable,
    EDATA_T extends Writable,
    IN_MSG_T extends Writable,
    OUT_MSG_T extends Writable, GS_VID_T> implements
    GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphNettyMessageManager.class);

    private ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf;
    private NetworkMap networkMap;

    private SendMessageCache outMessageCache;
    private NettyClient client;
    private NettyServer<OID_T, GS_VID_T> server;
    private SimpleFragment fragment;
    /**
     * Message store factory
     */
    private MessageStoreFactory<OID_T, IN_MSG_T, MessageStore<OID_T, IN_MSG_T, GS_VID_T>>
        messageStoreFactory;
    /**
     * Message store for incoming messages (messages which will be consumed in the next super step)
     */
    private volatile MessageStore<OID_T, IN_MSG_T, GS_VID_T> nextIncomingMessageStore;
    /**
     * Message store for current messages (messages which we received in previous super step and
     * which will be consumed in current super step)
     */
    private volatile MessageStore<OID_T, IN_MSG_T, GS_VID_T> currentIncomingMessageStore;

    private com.alibaba.graphscope.ds.Vertex grapeVertex;

    private int fragId, fragNum;

    private DefaultMessageManager grapeMessager;

    /**
     * The constructor is the preApplication.
     *
     * @param fragment   fragment to use
     * @param networkMap network map
     * @param conf       configuration
     */
    public GiraphNettyMessageManager(SimpleFragment fragment, NetworkMap networkMap,
        DefaultMessageManager mm,
        ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf) {
        this.fragment = fragment;
        this.networkMap = networkMap;
        this.conf = conf;
        this.grapeMessager = mm;
        this.fragId = fragment.fid();
        this.fragNum = fragment.fnum();
        outMessageCache = new BatchWritableMessageCache(fragNum, fragId, client, conf);

        initMessageStore();
        //Netty server depends on message store.
        initNetty();
        grapeVertex = FFITypeFactoryhelper.newVertex(conf.getGrapeVidClass());
    }

    public void initNetty() {
        logger.info("Creating server on " + networkMap.getSelfWorkerId() + " max bind time: "
            + MAX_IPC_PORT_BIND_ATTEMPTS.get(conf));
        server = new NettyServer(conf, fragment, networkMap, nextIncomingMessageStore,
            (Thread t, Throwable e) -> logger.error(t.getId() + ": " + e.toString()));
        server.startServer();

        logger.info("Create client on " + networkMap.getSelfWorkerId() + " max times: "
            + MAX_CONN_TRY_ATTEMPTS.get(conf));
        client = new NettyClient(conf, networkMap,
            (Thread t, Throwable e) -> logger.error(t.getId() + ": " + e.toString()));
        client.connectToAllAddress();
        logger.info(
            "Worker [" + networkMap.getSelfWorkerId() + "] listen on " + networkMap.getAddress()
                + ", client: " + client.toString());
    }

    public void initMessageStore() {
        messageStoreFactory = createMessageStoreFactory();
        nextIncomingMessageStore = messageStoreFactory.newStore(conf.getIncomingMessageClasses());
        currentIncomingMessageStore = messageStoreFactory
            .newStore(conf.getIncomingMessageClasses());
    }

    private MessageStoreFactory<OID_T, IN_MSG_T, MessageStore<OID_T, IN_MSG_T, GS_VID_T>> createMessageStoreFactory() {
        Class<? extends MessageStoreFactory> messageStoreFactoryClass =
            MESSAGE_STORE_FACTORY_CLASS.get(conf);

        MessageStoreFactory messageStoreFactoryInstance =
            ReflectionUtils.newInstance(messageStoreFactoryClass);
        messageStoreFactoryInstance.initialize(fragment, conf);

        return messageStoreFactoryInstance;
    }

    /**
     * Called by our framework, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {
        //No op
    }

    /**
     * Get the messages received from last round.
     *
     * @param lid local id.
     * @return received msg.
     */
    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        return (Iterable<IN_MSG_T>) currentIncomingMessageStore.getMessages(lid);
    }

    /**
     * Check any message available on this vertex.
     *
     * @param lid local id
     * @return true if recevied messages.
     */
    @Override
    public boolean messageAvailable(long lid) {
        return currentIncomingMessageStore.messageAvailable(lid);
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
            Long longOid = ((LongWritable) dstOid).get();
            // Get lid from oid

            boolean res = fragment.getVertex(longOid, grapeVertex);
            logger.debug(
                "oid 2 lid: res: " + res + " oid: " + longOid + ", " + grapeVertex.GetValue());

            int dstfragId = fragment.getFragId(grapeVertex);
            logger.debug(
                "Message manager sending via cache, dst frag: [" + dstfragId + "] dst gid: "
                    + fragment.vertex2Gid(grapeVertex) + "msg: " + message);
            outMessageCache
                .sendMessage(dstfragId, (Long) fragment.vertex2Gid(grapeVertex), message);
        } else {
            throw new IllegalStateException("Expect a long writable");
        }
    }

    /**
     * Send msg to all neighbors of vertex.
     *
     * @param vertex  querying vertex
     * @param message message to send.
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {
        VertexImpl<OID_T, VDATA_T, EDATA_T> vertexImpl =
            (VertexImpl<OID_T, VDATA_T, EDATA_T>) vertex;
        grapeVertex.SetValue(vertexImpl.getLocalId());

        // send msg through outgoing adjlist
        AdjList adjList = fragment.getOutgoingAdjList(grapeVertex);
        Iterable<Nbr> iterable = adjList.iterator();
        com.alibaba.graphscope.ds.Vertex<Long> curVertex;
        if (message instanceof LongWritable) {
            LongWritable longWritable = (LongWritable) message;
            for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
                Nbr nbr = it.next();
                curVertex = nbr.neighbor();
                int dstfragId = fragment.getFragId(curVertex);
                long gid = (long) fragment.vertex2Gid(curVertex);
                outMessageCache.sendMessage(dstfragId, gid, message);
            }
            // send msg through incoming adjlist
            adjList = fragment.getIncomingAdjList(grapeVertex);
            iterable = adjList.iterator();
            for (Iterator<Nbr> it = iterable.iterator(); it.hasNext(); ) {
                Nbr nbr = it.next();
                curVertex = nbr.neighbor();
                int dstfragId = fragment.getFragId(curVertex);
                long gid = (long) fragment.vertex2Gid(curVertex);
                outMessageCache.sendMessage(dstfragId, gid, message);
            }
        }

        logger.debug(
            "After send messages from vertex: "
                + grapeVertex.GetValue()
                + " through all edges");
        for (int i = 0; i < fragment.fnum(); ++i) {
//            logger.debug("To frag[" + i + "]: " + messagesOut[i].bytesWriten());
//            logger.debug("messages to frag [ " + i + "] " + );
        }
    }

    /**
     * Make sure all messages has been sent.
     */
    @Override
    public void finishMessageSending() {
//        client.waitAllRequests();
        outMessageCache.flushMessage();
    }

    /**
     * As this is called after superStep and before presuperStep's swapping, we check
     * nextIncomingMessage Store.
     *
     * @return true if message received
     */
    @Override
    public boolean anyMessageReceived() {
        return nextIncomingMessageStore.anyMessageReceived();
    }

    @Override
    public void forceContinue() {
        grapeMessager.ForceContinue();
    }

    @Override
    public void preSuperstep() {
        currentIncomingMessageStore.swap(nextIncomingMessageStore);
        nextIncomingMessageStore.clearAll();
        outMessageCache.clear();
    }

    @Override
    public void postSuperstep() {
        //wait for messages sent.
//        client.waitAllRequests();
        /** Get received message in server, in netty server */

        /** Add to self cache */
        outMessageCache.removeMessageToSelf(nextIncomingMessageStore);
//        currentIncomingMessageStore.clearAll();
        //Remove messages to self in cache, and send them to nextIncomingMessage store.
//        nextIncomingMessageStore.remo(outMessageCache.getMessageToSelf());
        //If client sent all messages, then server should have already resolved them since
        //we need the ack back.
    }

    @Override
    public void postApplication() {
        logger.info("Closing Client...");
        client.close();
        logger.info("Closing Server...");
        server.close();
    }
}
