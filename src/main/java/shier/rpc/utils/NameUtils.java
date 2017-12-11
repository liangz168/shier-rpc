package shier.rpc.utils;

import java.lang.reflect.Method;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-04 下午5:09
 **/
public class NameUtils {

    public static String buildServiceName(String interfaceName, String version) {
        return interfaceName + "_" + version;
    }

    public static String buildMethodName(Method method) {
        StringBuffer stringBuffer = new StringBuffer(method.getName());
        stringBuffer.append("(").append(method.getParameterCount()).append(")");
        return stringBuffer.toString();
    }

    public static String buildServiceMethodName(String serviceName, String methodName) {
        return serviceName + ":" + methodName;
    }

    public static String buildAddressName(String address, Integer port) {
        return address + ":" + port;
    }
}
