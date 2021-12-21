package org.apache.giraph.comm.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.Objects;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.netty.handler.NettyClientHandler;
import org.apache.giraph.comm.netty.handler.RequestDecoder;
import org.apache.giraph.comm.netty.handler.RequestEncoder;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation class which do responsible for all netty-related stuff.
 */
public class NettyClient {

    private static Logger logger = LoggerFactory.getLogger(NettyClient.class);
    /**
     * 30 seconds to connect by default
     */
    public static final int MAX_CONNECTION_MILLISECONDS_DEFAULT = 30 * 1000;

    public static final int SIZE = 256;

    private WorkerInfo workerInfo;
    private EventLoopGroup workGroup;
//    private ChannelFuture channelFuture;
    private Channel channel;
    private ImmutableClassesGiraphConfiguration conf;

    public NettyClient(ImmutableClassesGiraphConfiguration conf, WorkerInfo workerInfo,
        final Thread.UncaughtExceptionHandler exceptionHandler) {
        this.workerInfo = workerInfo;
        this.conf = conf;

        workGroup = new NioEventLoopGroup(4,
            ThreadUtils.createThreadFactory(
                "netty-client-worker-%d", exceptionHandler));
        try {
            Bootstrap b = new Bootstrap();
            b.option(ChannelOption.SO_KEEPALIVE,true);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.group(workGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new RequestEncoder(conf));
                        p.addLast(new RequestDecoder(conf));
                        p.addLast(new NettyClientHandler());
                    }
                });

            // Make the connection attempt.
            channel = b.connect(workerInfo.getHost(), workerInfo.getInitPort()).sync().channel();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(WritableRequest request){
	channel.writeAndFlush(request);
    }

    public void close(){
        try {
            if (!Objects.isNull(channel)){
                channel.close();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        workGroup.shutdownGracefully();
//        try {
//            channelFuture.channel().closeFuture().sync();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        logger.info("shut down client");
    }


}
