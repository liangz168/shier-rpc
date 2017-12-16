package shier.rpc.spring;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import shier.rpc.netty.RpcNettyClient;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-01 下午8:33
 **/
@Slf4j
public class RpcConfigBean {

    public static Integer CALLBACK_MAP_INIT_SIZE = 512;

    public static Integer RPC_DECODER_MAX_MAX_FRAME_LENGTH = Integer.MAX_VALUE;

    private static final String ZK_PATH = "/rpcService_";

    private static final String CENTER = "center";

    private String zookeeperAddress;

    private ZkClient zkClient;
    private Map<String, RpcNettyClient> rpcNettyClientMap = new HashMap<>();


    @PostConstruct
    public void init() {
        zkClient = new ZkClient(zookeeperAddress, 10000, 10000, new SerializableSerializer());
        log.info("zkClient connected! address={}", zookeeperAddress);
    }

    /**
     * 注册服务消费者
     *
     * @param serviceName
     * @param rpcConsumerMethodInterceptor
     */
    public void registerConsumer(String serviceName, RpcConsumerMethodInterceptor rpcConsumerMethodInterceptor) {
        String servicePath = ZK_PATH + serviceName;
        if (!zkClient.exists(servicePath)) {
            zkClient.create(servicePath, new ArrayList<>(), CreateMode.PERSISTENT);
        }

        List<String> addressList = zkClient.readData(servicePath, new Stat());
        log.info("registerConsumer serviceName={} address={}", serviceName, JSON.toJSONString(addressList));
        if (addressList != null && !addressList.isEmpty()) {
            for (String address : addressList) {
                RpcNettyClient rpcNettyClient = rpcNettyClientMap.get(address);
                if (rpcNettyClient == null) {
                    rpcNettyClient = new RpcNettyClient(address);
                    rpcNettyClient.init();
                    rpcNettyClientMap.put(address, rpcNettyClient);
                }
                rpcConsumerMethodInterceptor.getRpcNettyClientList().add(rpcNettyClient);
            }
        }
        //监听消费的服务变化
        zkClient.subscribeDataChanges(servicePath, new RpcZkListener(rpcConsumerMethodInterceptor, serviceName, addressList));
    }

    /**
     * 注册服务提供者
     *
     * @param serviceName
     * @param address
     */
    public void registerProvider(String serviceName, String address) {
        String servicePath = ZK_PATH + serviceName;
        if (zkClient.exists(servicePath)) {
            List<String> addressList = zkClient.readData(servicePath, new Stat());
            if (!addressList.contains(address)) {
                addressList.add(address);
                zkClient.writeData(servicePath, addressList);
            }
        } else {
            List<String> addressList = new ArrayList<>();
            addressList.add(address);
            zkClient.create(servicePath, addressList, CreateMode.PERSISTENT);//创建节点
        }
        log.info("registerProvider serviceName={} address={}", serviceName, address);
    }

    /**
     * 注销服务提供者
     *
     * @param serviceName
     * @param address
     */
    public void cancelProvider(String serviceName, String address) {
        String servicePath = ZK_PATH + serviceName;
        List<String> addressList = zkClient.readData(servicePath, new Stat());
        addressList.remove(address);
        zkClient.writeData(servicePath, addressList);
        log.info("cancelProvider serviceName={} address={}", serviceName, address);
    }

    /**
     * 注册服务监控中心
     *
     * @param address
     */
    public void registerCenter(String address) {
        String centerPath = ZK_PATH + CENTER;
        if (zkClient.exists(centerPath)) {
            zkClient.writeData(centerPath, address);
        } else {
            zkClient.create(centerPath, address, CreateMode.PERSISTENT);//创建节点
        }
    }


    /**
     * 注销服务监控中心
     */
    public void cancelCenter() {
        String centerPath = ZK_PATH + CENTER;
        if (zkClient.exists(centerPath)) {
            zkClient.writeData(centerPath, null);
        }
    }

    public String getCenterAddress() {
        String centerPath = ZK_PATH + CENTER;
        if (zkClient.exists(centerPath)) {
            return zkClient.readData(centerPath, new Stat());
        } else {
            return null;
        }
    }

    public void setZookeeperAddress(String zookeeperAddress) {
        this.zookeeperAddress = zookeeperAddress;
    }

    private class RpcZkListener implements IZkDataListener {

        private RpcConsumerMethodInterceptor rpcConsumerMethodInterceptor;

        private String serviceName;

        private List<String> addressList;

        public RpcZkListener(RpcConsumerMethodInterceptor rpcConsumerMethodInterceptor, String serviceName, List<String> addressList) {
            this.rpcConsumerMethodInterceptor = rpcConsumerMethodInterceptor;
            this.serviceName = serviceName;
            this.addressList = addressList;
        }

        @Override
        public void handleDataChange(String s, Object o) throws Exception {

            synchronized (this) {
                List<String> newAddressList = (List<String>) o;
                log.info("RpcZkListener path:{} change:{}", s, JSON.toJSONString(newAddressList));
                if (newAddressList == null) {
                    newAddressList = new ArrayList<>();
                }

                if (addressList == null) {
                    addressList = new ArrayList<>();
                }

                for (int i = addressList.size() - 1; i >= 0; i--) {
                    String address = addressList.get(i);
                    if (!newAddressList.contains(address)) { //有服务提供者下线
                        addressList.remove(i);
                        RpcNettyClient oldClient = rpcNettyClientMap.get(address);
                        rpcNettyClientMap.remove(address);
                        List<RpcNettyClient> rpcNettyClientList = rpcConsumerMethodInterceptor.getRpcNettyClientList().stream()
                                .filter(rpcNettyClient -> !rpcNettyClient.getServiceAddress().equals(address)).collect(Collectors.toList());
                        rpcConsumerMethodInterceptor.setRpcNettyClientList(rpcNettyClientList);
                        if (oldClient != null) {
                            oldClient.disConnect();
                        }

                    }
                }

                for (String address : newAddressList) {
                    if (!addressList.contains(address)) { //有新的服务提供者
                        addressList.add(address);
                        RpcNettyClient rpcNettyClient = rpcNettyClientMap.get(address);
                        if (rpcNettyClient == null) {
                            rpcNettyClient = new RpcNettyClient(address);
                            rpcNettyClient.init();
                            rpcNettyClientMap.put(address, rpcNettyClient);
                        }
                        rpcConsumerMethodInterceptor.getRpcNettyClientList().add(rpcNettyClient);
                    }
                }
            }


        }

        @Override
        public void handleDataDeleted(String s) throws Exception {

        }
    }

}
