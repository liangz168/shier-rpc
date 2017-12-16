package shier.rpc.spring;

import net.sf.cglib.proxy.Enhancer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import shier.rpc.utils.NameUtils;

import javax.annotation.PostConstruct;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-01 下午4:01
 **/
public class RpcConsumerFactoryBean implements FactoryBean {

    private String interfaceName;

    private String version;

    private Class objectType;

    private Object bean;

    private Long timeout = 5000L;

    @Autowired
    private RpcConfigBean rpcConfigBean;

    @PostConstruct
    public void init() throws ClassNotFoundException {
        if (interfaceName == null) {
            throw new NullPointerException("interfaceName can't be null");
        }

        if (rpcConfigBean == null) {
            throw new NullPointerException("rpcConfigBean can't be null");
        }

        this.objectType = Class.forName(interfaceName);
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(objectType);
        enhancer.setCallback(new RpcConsumerMethodInterceptor(NameUtils.buildServiceName(interfaceName, version), rpcConfigBean, timeout));
        this.bean = enhancer.create();
    }

    @Override
    public Object getObject() throws Exception {
        return bean;
    }

    @Override
    public Class getObjectType() {
        return objectType;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public RpcConsumerFactoryBean setInterfaceName(String interfaceName) {
        this.interfaceName = interfaceName;
        return this;
    }

    public RpcConsumerFactoryBean setVersion(String version) {
        this.version = version;
        return this;
    }

    public void setRpcConfigBean(RpcConfigBean rpcConfigBean) {
        this.rpcConfigBean = rpcConfigBean;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }
}
