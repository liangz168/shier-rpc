package shier.rpc.spring;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import shier.rpc.dto.RpcRequestDTO;
import shier.rpc.dto.RpcResponseDTO;
import shier.rpc.netty.HessianObjectDecoder;
import shier.rpc.netty.HessianObjectEncoder;
import shier.rpc.utils.NameUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-04 下午4:31
 **/
@Slf4j
public class RpcProviderBean implements Runnable {

    @Autowired
    private RpcConfigBean rpcConfigBean;

    private String version = "0.0.1";

    private String address;

    private Integer port = 8090;

    private List<Object> serviceList = new ArrayList<>();

    private Map<String, Object> serviceMap = new HashMap<>();

    private Map<String, Method> serviceMethodMap = new HashMap<>();

    private Integer corePoolSize = 50;

    private Integer maxPoolSize = 600;

    private Integer keepAliveSeconds = 600;

    private ThreadPoolTaskExecutor taskExecutor;


    @PostConstruct
    public void init() throws Exception {
        if (StringUtil.isNullOrEmpty(address)) {
            address = InetAddress.getLocalHost().getHostAddress();
        }

        if (taskExecutor == null) { // 初始化线程池
            taskExecutor = new ThreadPoolTaskExecutor();
            taskExecutor.setCorePoolSize(corePoolSize);
            taskExecutor.setKeepAliveSeconds(maxPoolSize);
            taskExecutor.setMaxPoolSize(keepAliveSeconds);
            taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
            taskExecutor.initialize();
        }

        for (Object service : serviceList) {
            Class<?>[] interfaces = service.getClass().getInterfaces();
            for (Class clazz : interfaces) {
                String serviceName = NameUtils.buildServiceName(clazz.getName(), version);
                serviceMap.put(serviceName, service);
                for (Method method : clazz.getMethods()) {
                    String methodName = NameUtils.buildMethodName(method);
                    String serviceMethodName = NameUtils.buildServiceMethodName(serviceName, methodName);
                    if (serviceMethodMap.containsKey(serviceMethodName)) {
                        throw new Exception(serviceName + " have repeat method " + methodName);
                    }
                    serviceMethodMap.put(serviceMethodName, method);
                }
            }
        }

        new Thread(this).start();
    }

    @Override
    public void run() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(RpcConfigBean.RPC_DECODER_MAX_MAX_FRAME_LENGTH, 0, 4, 0, 4));
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new HessianObjectEncoder());
                            ch.pipeline().addLast(new HessianObjectDecoder(Integer.MAX_VALUE));
                            ch.pipeline().addLast(new ProviderServerHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            log.info("RpcProviderBean start!");
            registerProvider();
            ChannelFuture f = b.bind(port).sync();
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("", e);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @PreDestroy
    public void destroy() {
        log.info("RpcProviderBean stop!");
        cancelProvider();
        if (taskExecutor != null) {
            taskExecutor.shutdown();
        }
    }

    private void registerProvider() {
        String addressName = NameUtils.buildAddressName(address, port);
        serviceMap.keySet().forEach(serviceName -> {
            rpcConfigBean.registerProvider(serviceName, addressName);
        });
    }

    private void cancelProvider() {
        String addressName = NameUtils.buildAddressName(address, port);
        serviceMap.keySet().forEach(serviceName -> {
            rpcConfigBean.cancelProvider(serviceName, addressName);
        });
    }

    public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setCorePoolSize(Integer corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public void setMaxPoolSize(Integer maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public void setKeepAliveSeconds(Integer keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }

    private class ProviderServerHandler extends ChannelInboundHandlerAdapter {

        private Channel channel;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws UnsupportedEncodingException {
            invoke((RpcRequestDTO) msg);
        }

        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelReadComplete();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("ProviderServerHandler.exceptionCaught", cause);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.channel = ctx.channel();
        }

        private void invoke(RpcRequestDTO rpcRequestDTO) {
            taskExecutor.execute(() -> {
                Object service = serviceMap.get(rpcRequestDTO.getServiceName());
                if (service == null) {
                    this.returnError(rpcRequestDTO.getRequestId(), new Exception(rpcRequestDTO.getServiceName() + " can't find provider "));
                    return;
                }

                Method method = serviceMethodMap.get(NameUtils.buildServiceMethodName(rpcRequestDTO.getServiceName(), rpcRequestDTO.getMethodName()));
                if (method == null) {
                    this.returnError(rpcRequestDTO.getRequestId(), new Exception(rpcRequestDTO.getServiceName() + " can't find method " + rpcRequestDTO.getMethodName()));
                    return;
                }

                try {
                    Object result = method.invoke(service, rpcRequestDTO.getParams());
                    this.returnResponse(rpcRequestDTO.getRequestId(), result);
                } catch (Exception e) {
                    log.error("", e);
                    this.returnError(rpcRequestDTO.getRequestId(), e);
                }
            });
        }

        private void returnResponse(String requestId, Object object) {
            RpcResponseDTO rpcResponseDTO = new RpcResponseDTO();
            rpcResponseDTO.setRequestId(requestId);
            rpcResponseDTO.setResult(object);
            channel.writeAndFlush(rpcResponseDTO);
        }

        private void returnError(String requestId, Exception e) {
            RpcResponseDTO rpcResponseDTO = new RpcResponseDTO();
            rpcResponseDTO.setRequestId(requestId);
            rpcResponseDTO.setHasError(true);
            rpcResponseDTO.setThrowable(e);
            channel.writeAndFlush(rpcResponseDTO);
        }

    }

    public void setRpcConfigBean(RpcConfigBean rpcConfigBean) {
        this.rpcConfigBean = rpcConfigBean;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setServiceList(List<Object> serviceList) {
        this.serviceList = serviceList;
    }

}
