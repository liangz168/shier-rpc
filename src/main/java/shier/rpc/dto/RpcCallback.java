package shier.rpc.dto;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import shier.rpc.exception.RpcTimeoutException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-02 下午8:32
 **/
@Data
@Slf4j
public class RpcCallback {
    private Lock lock = new ReentrantLock();
    private Condition finish = lock.newCondition();
    private String requestId;
    private Long timeout;

    private Object object;

    public RpcCallback() {
    }

    public RpcCallback(String requestId, Long timeout) {
        this.requestId = requestId;
        this.timeout = timeout;
    }

    public Object waitCallback() throws Exception {

        if (this.object != null) {
            return this.object;
        }

        try {
            lock.lock();
            Boolean signalFlg = finish.await(timeout, TimeUnit.MILLISECONDS);
            if (!signalFlg) {
                throw new RpcTimeoutException("await timeout " + timeout);
            }
            return this.object;
        } finally {
            lock.unlock();
        }
    }

    public void callback(Object object) {
        try {
            lock.lock();
            this.object = object;
            finish.signalAll();
        } finally {
            lock.unlock();
        }
    }

}
