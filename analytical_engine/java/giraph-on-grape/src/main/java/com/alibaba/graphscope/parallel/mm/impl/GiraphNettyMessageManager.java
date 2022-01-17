package com.alibaba.graphscope.parallel.mm.impl;

import static org.apache.giraph.conf.GiraphConstants.MAX_CONN_TRY_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.message.MessageStoreFactory;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import com.alibaba.graphscope.parallel.netty.NettyServer;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
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
    OUT_MSG_T extends Writable, GS_VID_T, GS_OID_T> implements
    GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T,GS_OID_T> {

    private static Logger logger = LoggerFactory.getLogger(GiraphNettyMessageManager.class);

    private ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf;
    private NetworkMap networkMap;

    private SendMessageCache<OID_T, OUT_MSG_T, GS_VID_T> outMessageCache;
    private NettyClient client;
    private NettyServer<OID_T, GS_VID_T> server;
    private SimpleFragment<GS_OID_T, GS_VID_T,?,?> fragment;
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

    private com.alibaba.graphscope.ds.Vertex<GS_VID_T> grapeVertex;

    private int fragId, fragNum;

    private DefaultMessageManager grapeMessager;

    private Class<? extends GS_OID_T> gsOidClass;

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
        this.gsOidClass = (Class<? extends GS_OID_T>) conf.getGrapeOidClass();

        initMessageStore();
        //Netty server depends on message store.
        initNetty();

        // Create different type of message cache as needed.
        outMessageCache = (SendMessageCache<OID_T, OUT_MSG_T, GS_VID_T>) SendMessageCache.newMessageCache(
            fragNum, fragId, client, conf);
        grapeVertex = (com.alibaba.graphscope.ds.Vertex<GS_VID_T>) FFITypeFactoryhelper.newVertex(conf.getGrapeVidClass());
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
        return currentIncomingMessageStore.getMessages(lid);
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
            if (!fragment.getVertex((GS_OID_T) longOid, grapeVertex)){
                throw new IllegalStateException("get lid failed for oid: " + longOid);
            }
            sendLidMessage(grapeVertex, message);
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
        grapeVertex.SetValue((GS_VID_T) (Long) vertexImpl.getLocalId());

        // send msg through outgoing adjlist
        for (Nbr<GS_VID_T,?> nbr : fragment.getOutgoingAdjList(grapeVertex).iterable()){
            com.alibaba.graphscope.ds.Vertex<GS_VID_T> curVertex = nbr.neighbor();
            sendLidMessage(curVertex, message);
        }
        for (Nbr<GS_VID_T,?> nbr : fragment.getIncomingAdjList(grapeVertex).iterable()){
            com.alibaba.graphscope.ds.Vertex<GS_VID_T> curVertex = nbr.neighbor();
            sendLidMessage(curVertex, message);
        }
    }

    private void sendLidMessage(com.alibaba.graphscope.ds.Vertex<GS_VID_T> nbrVertex, OUT_MSG_T message){
        int dstfragId = fragment.getFragId(nbrVertex);
        if (logger.isDebugEnabled()){
            logger.debug("worker [{}] send msg {} to vertex {} on worker {}", fragId, message, nbrVertex.GetValue(), dstfragId);
        }
        outMessageCache.sendMessage(dstfragId, fragment.vertex2Gid(nbrVertex), message);
    }

    /**
     * Make sure all messages has been sent.
     */
    @Override
    public void finishMessageSending() {
        outMessageCache.flushMessage();
        /** Add to self cache, IN_MSG_T must be same as OUT_MSG_T */
        outMessageCache.removeMessageToSelf(
            (MessageStore<OID_T, OUT_MSG_T, GS_VID_T>) nextIncomingMessageStore);
    }

    /**
     * As this is called after superStep and before presuperStep's swapping, we check
     * nextIncomingMessage Store.
     *
     * @return true if message received
     */
    @Override
    public boolean anyMessageReceived() {
        return currentIncomingMessageStore.anyMessageReceived();
    }

    @Override
    public void forceContinue() {
        grapeMessager.ForceContinue();
    }

    @Override
    public void preSuperstep() {
        server.preSuperStep((MessageStore<OID_T, Writable, GS_VID_T>) nextIncomingMessageStore);
    }

    @Override
    public void postSuperstep() {
        //First wait all message arrived.
        client.postSuperStep();
        outMessageCache.clear();
        currentIncomingMessageStore.swap(nextIncomingMessageStore);
        nextIncomingMessageStore.clearAll();
    }

    @Override
    public void postApplication() {
        logger.info("Closing Client...");
        client.close();
        logger.info("Closing Server...");
        server.close();
    }
}
