package shier.rpc.spring;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import shier.rpc.dto.RpcRequestDTO;
import shier.rpc.netty.RpcNettyClient;
import shier.rpc.utils.NameUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-01 下午4:06
 **/
public class RpcConsumerMethodInterceptor implements MethodInterceptor {

    private String serviceName;

    private Long timeout;

    private RpcConfigBean rpcConfigBean;

    private List<RpcNettyClient> rpcNettyClientList = new ArrayList<>();

    public RpcConsumerMethodInterceptor(String serviceName, RpcConfigBean rpcConfigBean, Long timeout) {
        this.serviceName = serviceName;
        this.rpcConfigBean = rpcConfigBean;
        this.timeout = timeout;
        rpcConfigBean.registerConsumer(serviceName, this);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (rpcNettyClientList.isEmpty()) {
            throw new Exception(serviceName + " have not a provider !");
        }

        RpcRequestDTO rpcRequestDTO = new RpcRequestDTO();
        rpcRequestDTO.setRequestId(UUID.randomUUID().toString().substring(0, 16));
        rpcRequestDTO.setServiceName(serviceName);
        rpcRequestDTO.setMethodName(NameUtils.buildMethodName(method));
        rpcRequestDTO.setParams(args);

        List<RpcNettyClient> list = rpcNettyClientList;
        int size = list.size();
        int index = (int) (Math.random() * size); // 随机负载
        return list.get(index).sendRpcRequest(rpcRequestDTO, timeout);
    }

    public String getServiceName() {
        return serviceName;
    }

    public RpcConsumerMethodInterceptor setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public List<RpcNettyClient> getRpcNettyClientList() {
        return rpcNettyClientList;
    }

    public RpcConsumerMethodInterceptor setRpcNettyClientList(List<RpcNettyClient> rpcNettyClientList) {
        this.rpcNettyClientList = rpcNettyClientList;
        return this;
    }
}
