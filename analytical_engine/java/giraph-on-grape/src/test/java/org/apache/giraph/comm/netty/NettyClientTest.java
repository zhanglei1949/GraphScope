package org.apache.giraph.comm.netty;

import static org.mockito.Mockito.mock;

import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.requests.AggregatorMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

public class NettyClientTest {

    private static Logger logger = LoggerFactory.getLogger(NettyClientTest.class);

    private NettyServer server;
    private NettyClient client;
    private ImmutableClassesGiraphConfiguration configuration;
    private WorkerInfo workerInfo;

    @Before
    public void prepare() {
        ImmutableClassesGiraphConfiguration conf = mock(ImmutableClassesGiraphConfiguration.class);
        AggregatorManager aggregatorManager = mock(AggregatorManagerNettyImpl.class);
        //        when(conf.)
        workerInfo = new WorkerInfo(0, 1, "0.0.0.0", 30000, null);
        server =
                new NettyServer(
                        conf,
                        aggregatorManager,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
        client =
                new NettyClient(
                        conf,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
    }

    @Test
    public void test() {
        for (int i = 0; i < 10; ++i) {
            //            SimpleLongWritableRequest writable = new SimpleLongWritableRequest(new
            // LongWritable(i));
            byte[] bytes = new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)};
            AggregatorMessage aggregatorMessage = new AggregatorMessage("Agg1", bytes);
            client.sendMessage(aggregatorMessage);
        }
    }

    @After
    public void close() {
        // server.close();
        client.close();
    }
}
