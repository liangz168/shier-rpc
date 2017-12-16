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

    private Integer corePoolSize = 200;

    private Integer maxPoolSize = 800;

    private Integer keepAliveSeconds = 3000;

    private ThreadPoolTaskExecutor taskExecutor;

    public static final String HEALTH = "health";

    public static final String OK = "ok";

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        if (StringUtil.isNullOrEmpty(address)) {
            address = InetAddress.getLocalHost().getHostAddress();
        }

        // 初始化线程池
        if (taskExecutor == null) {
            taskExecutor = new ThreadPoolTaskExecutor();
            taskExecutor.setThreadNamePrefix("shier-rpc");
            taskExecutor.setCorePoolSize(corePoolSize);
            taskExecutor.setKeepAliveSeconds(keepAliveSeconds);
            taskExecutor.setMaxPoolSize(maxPoolSize);
            taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
            taskExecutor.initialize();
        }

        //服务方法映射
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
        //启动netty服务端
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
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

    /**
     * 关闭服务
     */
    @PreDestroy
    public void destroy() {
        log.info("RpcProviderBean stop!");
        cancelProvider();

        if (taskExecutor != null) {
            taskExecutor.shutdown();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * 注册服务提供者
     */
    private void registerProvider() {
        String addressName = NameUtils.buildAddressName(address, port);
        serviceMap.keySet().forEach(serviceName -> {
            rpcConfigBean.registerProvider(serviceName, addressName);
        });
    }

    /**
     * 注销服务提供者
     */
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
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            RpcRequestDTO rpcRequestDTO = (RpcRequestDTO) msg;
            try {
                invoke(rpcRequestDTO);
            } catch (Exception e) {
                log.error("ProviderServerHandler.invoke", e);
                this.returnError(rpcRequestDTO.getRequestId(), e);
            }

        }

        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.fireChannelReadComplete();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("ProviderServerHandler.exceptionCaught", cause);
            ctx.close();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            this.channel = ctx.channel();
        }

        private void invoke(RpcRequestDTO rpcRequestDTO) {

            taskExecutor.execute(() -> {
                //处理服务调用
                Object service = serviceMap.get(rpcRequestDTO.getServiceName());
                if (service == null) {
                    //处理健康检查
                    if (HEALTH.equals(rpcRequestDTO.getServiceName()) && HEALTH.equals(rpcRequestDTO.getMethodName())) {
                        this.returnResponse(rpcRequestDTO.getRequestId(), OK);
                        return;
                    }

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

        /**
         * 返回调用结果
         *
         * @param requestId
         * @param object
         */
        private void returnResponse(String requestId, Object object) {
            RpcResponseDTO rpcResponseDTO = new RpcResponseDTO();
            rpcResponseDTO.setRequestId(requestId);
            rpcResponseDTO.setResult(object);
            channel.writeAndFlush(rpcResponseDTO);
        }

        /**
         * 调用出错返回异常
         *
         * @param requestId
         * @param e
         */
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
