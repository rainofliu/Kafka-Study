package com.ajin.kafkastudy;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @Author: ajin
 * @Date: 2019/4/26 12:09
 * 反序列化器
 */

public class UserDeserializer implements Deserializer<User> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {

        return JSON.parseObject(data, User.class);
    }

    @Override
    public void close() {

    }
}
