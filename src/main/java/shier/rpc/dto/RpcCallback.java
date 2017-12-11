package shier.rpc.dto;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

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

    private Object object;

    public RpcCallback() {
    }

    public Object waitCallback() throws InterruptedException {
        try {
            lock.lock();
            finish.await(5, TimeUnit.SECONDS);
            return this.object;
        } finally {
            lock.unlock();
        }
    }

    public void callback(Object object) {
        try {
            lock.lock();
            this.object = object;
            finish.signal();
        } finally {
            lock.unlock();
        }
    }

}
