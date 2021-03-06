package shier.rpc.netty;

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import shier.rpc.dto.RpcCallback;
import shier.rpc.dto.RpcRequestDTO;
import shier.rpc.dto.RpcResponseDTO;
import shier.rpc.exception.RpcConnectException;
import shier.rpc.spring.RpcConfigBean;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-01 下午8:23
 **/
@Slf4j
public class RpcNettyClient {

    private String serviceAddress;

    private ChannelFuture channelFuture;

    private RpcClientHandler rpcClientHandler;

    private EventLoopGroup group;

    public RpcNettyClient(String serviceAddress) {
        this.serviceAddress = serviceAddress;
    }

    /**
     * 发送远程调用请求
     *
     * @param rpcRequestDTO
     * @param timeout
     * @return
     * @throws Exception
     */
    public Object sendRpcRequest(RpcRequestDTO rpcRequestDTO, Long timeout) throws Exception {
        if (!channelFuture.isSuccess()) {
            throw new RpcConnectException(serviceAddress + " is cann't connect");
        }
        if (rpcClientHandler == null) {
            throw new NullPointerException(serviceAddress + " is cann't connect");
        }
        return rpcClientHandler.sendRpcRequest(rpcRequestDTO, timeout);
    }

    public void init() {
        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();

        ChannelFutureListener channelFutureListener = new MyChannelFutureListener(bootstrap);
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(RpcConfigBean.RPC_DECODER_MAX_MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(new HessianObjectEncoder());
                        ch.pipeline().addLast(new HessianObjectDecoder(Integer.MAX_VALUE));
                        rpcClientHandler = new RpcClientHandler(bootstrap, channelFutureListener);
                        ch.pipeline().addLast(rpcClientHandler);
                    }
                });

        doConnect(bootstrap, channelFutureListener);
    }

    /**
     * 连接服务
     *
     * @param bootstrap
     * @param channelFutureListener
     */
    private void doConnect(Bootstrap bootstrap, ChannelFutureListener channelFutureListener) {
        //发起异步链接操作
        String[] addressArray = serviceAddress.split(":");
        channelFuture = bootstrap.connect(addressArray[0], Integer.parseInt(addressArray[1]));
        channelFuture.addListener(channelFutureListener);
    }

    /**
     * 断开连接
     */
    public void disConnect() {
        log.info("RpcNettyClient.disConnect serviceAddress={}", serviceAddress);
        group.shutdownGracefully();
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public RpcNettyClient setServiceAddress(String serviceAddress) {
        this.serviceAddress = serviceAddress;
        return this;
    }

    private class RpcClientHandler extends ChannelInboundHandlerAdapter {

        private Map<String, RpcCallback> callbackMap = new HashMap<>(RpcConfigBean.CALLBACK_MAP_INIT_SIZE);

        private Channel channel;

        private Bootstrap bootstrap;

        private ChannelFutureListener channelFutureListener;

        RpcClientHandler(Bootstrap bootstrap, ChannelFutureListener channelFutureListener) {
            this.bootstrap = bootstrap;
            this.channelFutureListener = channelFutureListener;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.channel = ctx.channel();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelInactive();
            doConnect(bootstrap, channelFutureListener);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RpcResponseDTO responseDTO = (RpcResponseDTO) msg;
            RpcCallback rpcCallback = callbackMap.get(responseDTO.getRequestId());
            if (rpcCallback != null) {
                log.debug("收到回调requestId={} result={}", responseDTO.getRequestId(), responseDTO.getResult());
                callbackMap.remove(responseDTO.getRequestId());
                rpcCallback.callback(responseDTO.getResult()); //收到回调
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("RpcClientHandler.exceptionCaught", cause);
            ctx.close();
        }

        private Object sendRpcRequest(RpcRequestDTO rpcRequestDTO, Long timeout) throws Exception {
            if (!this.channel.isActive()) {
                throw new RpcConnectException(serviceAddress + " is unable to connect");
            }
            try {
                RpcCallback rpcCallback = new RpcCallback(rpcRequestDTO.getRequestId(), timeout);
                callbackMap.put(rpcRequestDTO.getRequestId(), rpcCallback);
                this.channel.writeAndFlush(rpcRequestDTO); //发送请求
                return rpcCallback.waitCallback(); //等待回调返回结果
            } catch (Exception e) {
                callbackMap.remove(rpcRequestDTO.getRequestId());
                log.error("rpcRequest:{} 调用失败 {}", JSON.toJSONString(rpcRequestDTO), e.getMessage());
                throw e;
            }
        }
    }

    private class MyChannelFutureListener implements ChannelFutureListener {
        private Bootstrap bootstrap;

        public MyChannelFutureListener(Bootstrap bootstrap) {
            this.bootstrap = bootstrap;
        }

        public void operationComplete(ChannelFuture f) throws Exception {
            ChannelFutureListener _this = this;
            if (f.isSuccess()) { //连接成功
                log.info("RpcNettyClient serviceAddress={} connected!", serviceAddress);
            } else {
                //  连接失败 1秒后重新连接
                f.channel().eventLoop().schedule(() -> {
                    try {
                        doConnect(bootstrap, _this);
                    } catch (Exception e) {
                        log.error("MyChannelFutureListener.operationComplete", e);
                    }
                }, 1, TimeUnit.SECONDS);
            }
        }
    }

}
