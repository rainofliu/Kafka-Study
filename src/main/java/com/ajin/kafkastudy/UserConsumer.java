package com.ajin.kafkastudy;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author: ajin
 * @Date: 2019/4/26 12:06
 */

public class UserConsumer {


    private static KafkaConsumer<String, User> consumer;

    private static Properties properties;

    static {

        properties = new Properties();
        // kafka集群的地址
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        // key反序列化器
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        // value反序列化器
        properties.put("value.deserializer",
                "com.ajin.kafkastudy.UserDeserializer");
        // group.id 设置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaStudy");
    }

    /**
     * 异步提交offset位移信息
     */
    private static void generalMessageAsync() {

        consumer = new KafkaConsumer<String, User>(properties);

        properties.put("auto.commit.offset", false);

        consumer.subscribe(Collections.singleton("test1"));
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(100);

            for (ConsumerRecord<String, User> record : records) {
                // 打印消息详细信息
                System.out.printf(
                        "topic = %s,partition = %s,key = %s,value = %s\n",
                        record.topic(), record.partition(),
                        record.key(), record.value()
                );
            }
            try {
                consumer.commitAsync();
            } catch (Exception e) {
                System.out.println("commit offset async error");
            }

        }

    }

    public static void main(String[] args) {
        generalMessageAsync();
    }


}
