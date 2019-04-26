package com.ajin.kafkastudy;



import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Author: ajin
 * @Date: 2019/4/26 11:53
 * 该对象要被序列化后，发给Kafka
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User implements Serializable {

    private String username;
    private String password;
    private Integer age;
}
