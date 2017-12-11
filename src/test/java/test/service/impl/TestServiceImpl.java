package test.service.impl;

import test.dto.TestDTO;
import test.service.TestService;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-11 下午5:19
 **/

public class TestServiceImpl implements TestService {
    @Override
    public Long addTest(TestDTO testDTO) {
        System.out.println("addTest" + testDTO.getName());
        return System.currentTimeMillis();
    }
}
