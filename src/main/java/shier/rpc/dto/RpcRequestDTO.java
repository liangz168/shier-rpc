package shier.rpc.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-02 下午3:27
 **/
@Data
public class RpcRequestDTO implements Serializable {

    private String requestId;

    private String serviceName;

    private String methodName;

    private Object[] params;

}
