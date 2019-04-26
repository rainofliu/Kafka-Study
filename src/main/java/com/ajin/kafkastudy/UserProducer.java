package com.ajin.kafkastudy;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Author: ajin
 * @Date: 2019/4/26 12:01
 */

public class UserProducer {

    private static KafkaProducer<String, User> producer;

    static {

        Properties props = new Properties();

        // 生产者与Kafka建立初始连接的Broker列表；生产者与某一个Broker建立连接后，会自动获取到整个Kafka集群的信息
        // 最好采用两个Broker，防止一个Broker宕机出现异常
        props.put("bootstrap.servers", "127.0.0.1:9092");
        // key序列化器
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化器
        props.put("value.serializer",
                "com.ajin.kafkastudy.CustomSerializer");
        // 配置消息的分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.ajin.kafkastudy.CustomPartitioner");

        producer = new KafkaProducer<String, User>(props);

    }

    private static void sendMessageAsync() {

        User user = new User();
        user.setUsername("kafka user");
        user.setPassword("random");
        user.setAge(1024);

        ProducerRecord<String, User> record = new ProducerRecord<>("test1", "name", user);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                    return;
                } else {
                    System.out.println(metadata.topic());
                    System.out.println(metadata.partition());
                    System.out.println(metadata.offset());

                    System.out.println("Coming in sendMessageAsync");
                }
            }
        });

        producer.close();

    }

    public static void main(String[] args) {
        sendMessageAsync();
    }

}
