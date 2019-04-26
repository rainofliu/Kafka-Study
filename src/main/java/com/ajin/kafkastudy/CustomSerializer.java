package com.ajin.kafkastudy;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @Author: ajin
 * @Date: 2019/4/26 11:52
 * <p>
 * 自定义的序列化器，实现对对象的序列化
 */

public class CustomSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User data) {



        return JSON.toJSONBytes(data);
    }

    @Override
    public void close() {

    }
}
