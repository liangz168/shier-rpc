package test;

import junit.framework.Assert;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import test.dto.TestDTO;
import test.service.TestService;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-11 下午5:24
 **/
public class ApplicationTest {


    @Test
    public void test() throws InterruptedException {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"spring.xml"});
        context.start();
        Thread.sleep(5000L);
        TestService testService = (TestService) context.getBean("testService");
        TestDTO testDTO = new TestDTO();
        testDTO.setName("Hi shier-simple-rpc");
        Long res = testService.addTest(testDTO);
        Assert.assertTrue(res > 0);
    }

}
