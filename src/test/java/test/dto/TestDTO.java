package test.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author liangliang.wei
 * @description
 * @create 2017-12-11 下午5:18
 **/
@Data
public class TestDTO implements Serializable {

    private Long id;

    private String name;

}
