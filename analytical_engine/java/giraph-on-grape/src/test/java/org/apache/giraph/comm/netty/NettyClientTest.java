package org.apache.giraph.comm.netty;

import static org.mockito.Mockito.mock;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.requests.AggregatorMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.giraph.comm.requests.NettyMessage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.lang.Thread.UncaughtExceptionHandler;

public class NettyClientTest {

    private static Logger logger = LoggerFactory.getLogger(NettyClientTest.class);

    private NettyServer server;
    private NettyClient client;
    private ImmutableClassesGiraphConfiguration configuration;
    private WorkerInfo workerInfo;
    private AggregatorManager aggregatorManager;

    @Before
    public void prepare() throws InstantiationException, IllegalAccessException {
        ImmutableClassesGiraphConfiguration conf = mock(ImmutableClassesGiraphConfiguration.class);
        aggregatorManager = new AggregatorManagerNettyImpl(conf, 0,1);
        aggregatorManager.registerAggregator("sum", LongSumAggregator.class);
        aggregatorManager.setAggregatedValue("sum", new LongWritable(0));
        //        when(conf.)
        workerInfo = new WorkerInfo(0, 1, "172.17.0.14", 30000, null);
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
                        aggregatorManager,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
    }

    @Test
    public void test() throws InterruptedException, ExecutionException{
        for (int i = 0; i < 10; ++i) {
            //            SimpleLongWritableRequest writable = new SimpleLongWritableRequest(new
            // LongWritable(i));
//            byte[] bytes = new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)};
            byte[] bytes = new byte[] {(byte) 0, (byte) 0 , (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0 ,(byte) i};
            AggregatorMessage aggregatorMessage = new AggregatorMessage("sum", ""+i, bytes);
            Future<NettyMessage> msg =client.sendMessage(aggregatorMessage);
            while (!msg.isDone()){
                TimeUnit.SECONDS.sleep(1);
            }
	    logger.info(""+msg.get());
        }
    }

    @After
    public void close() {
        client.close();
        server.close();
    }
}
