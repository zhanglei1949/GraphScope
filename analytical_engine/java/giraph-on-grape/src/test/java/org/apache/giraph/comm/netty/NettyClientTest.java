package org.apache.giraph.comm.netty;

import java.lang.Thread.UncaughtExceptionHandler;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyClientTest {
    private static Logger logger = LoggerFactory.getLogger(NettyClientTest.class);

    private NettyServer server;
    private NettyClient client;
    private ImmutableClassesGiraphConfiguration configuration;
    private WorkerInfo workerInfo;

    @Before
    public void prepare(){
        workerInfo = new WorkerInfo(0,1, "127.0.0.1", 30000, null);
        server = new NettyServer(workerInfo, new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error(t.getId() + ": " + e.toString());
            }
        });
        client = new NettyClient(workerInfo, new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error(t.getId() + ": " + e.toString());
            }
        });
    }

    @Test
    public void test(){
        client.sendMessage();
    }

    @After
    public void close(){
        client.close();
        server.close();
    }

}
