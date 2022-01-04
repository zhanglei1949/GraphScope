package com.alibaba.graphscope.parallel.mm.impl;

import static org.apache.giraph.conf.GiraphConstants.MAX_CONN_TRY_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;

import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.cache.impl.BatchWritableMessageCache;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import com.alibaba.graphscope.parallel.netty.NettyServer;
import com.alibaba.graphscope.parallel.utils.NetworkMap;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
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
 *     Has similar role with WorkerClientRequestProcessor in Giraph.
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
    OUT_MSG_T extends Writable> implements
    GiraphMessageManager<OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T> {
    private static Logger logger = LoggerFactory.getLogger(GiraphNettyMessageManager.class);

    private ImmutableClassesGiraphConfiguration<OID_T,VDATA_T,EDATA_T> conf;
    private NetworkMap networkMap;

    private SendMessageCache outMessageCache;
    private NettyClient client;
    private NettyServer server;
    private SimpleFragment fragment;


    public GiraphNettyMessageManager(SimpleFragment fragment, NetworkMap networkMap, ImmutableClassesGiraphConfiguration<OID_T,VDATA_T,EDATA_T> conf){
        this.fragment = fragment;
        this.networkMap = networkMap;
        this.conf = conf;
        outMessageCache = new BatchWritableMessageCache();

        init();
    }

    public void init(){
        logger.info("Creating server on " + networkMap.getSelfWorkerId() + " max bind time: " + MAX_IPC_PORT_BIND_ATTEMPTS.get(conf));
        server = new NettyServer(conf, networkMap, (Thread t, Throwable e) -> logger.error(t.getId() + ": " + e.toString()));
        server.startServer();

        logger.info("Create client on " + networkMap.getSelfWorkerId() + " max times: " + MAX_CONN_TRY_ATTEMPTS.get(conf));
        client = new NettyClient(conf, networkMap, (Thread t, Throwable e) -> logger.error(t.getId() + ": " + e.toString()));
        client.connectToAllAddress();
        logger.info("Worker [" + networkMap.getSelfWorkerId() + "] listen on " + networkMap.getAddress() + ", client: " + client.toString());
    }

    /**
     * Called by our framework, to deserialize the messages from c++ to java. Must be called before
     * getMessages
     */
    @Override
    public void receiveMessages() {

    }

    /**
     * Get the messages received from last round.
     *
     * @param lid local id.
     * @return received msg.
     */
    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        return null;
    }

    /**
     * Check any message available on this vertex.
     *
     * @param lid local id
     * @return true if recevied messages.
     */
    @Override
    public boolean messageAvailable(long lid) {
        return false;
    }

    /**
     * Send one message to dstOid.
     *
     * @param dstOid  vertex to receive this message.
     * @param message message.
     */
    @Override
    public void sendMessage(OID_T dstOid, OUT_MSG_T message) {

    }

    /**
     * Send msg to all neighbors of vertex.
     *
     * @param vertex  querying vertex
     * @param message message to send.
     */
    @Override
    public void sendMessageToAllEdges(Vertex<OID_T, VDATA_T, EDATA_T> vertex, OUT_MSG_T message) {

    }

    /**
     * Make sure all messages has been sent.
     */
    @Override
    public void finishMessageSending() {

    }

    /**
     * Check any messages to self.
     *
     * @return true if messages sent to self.
     */
    @Override
    public boolean anyMessageToSelf() {
        return false;
    }

    @Override
    public void forceContinue() {

    }

    @Override
    public void postApplication(){
        logger.info("Closing Client...");
        client.close();
        logger.info("Closing Server...");
        server.close();
    }
}
