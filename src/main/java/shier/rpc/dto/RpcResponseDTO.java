package shier.rpc.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-04 下午7:08
 **/
@Data
public class RpcResponseDTO implements Serializable {
    private String requestId;
    private boolean hasError = false;
    private Throwable throwable;
    private Object result;
}
