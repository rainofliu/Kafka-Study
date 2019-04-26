package com.ajin.kafkastudy;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Author: ajin
 * @Date: 2019/4/26 08:50
 */

public class MyProducer {

    private static KafkaProducer<String, String> producer;

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
                "org.apache.kafka.common.serialization.StringSerializer");
        // 配置消息的分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.ajin.kafkastudy.CustomPartitioner");

        producer = new KafkaProducer<String, String>(props);

    }

    /**
     * 只管发送，不管Broker有没有收到
     **/
    private static void sendMessageForgetResult() {

        // 创建ProducerRecord
        // 消息的key和value是任意类型的，序列化器对应即可
        ProducerRecord<String, String> record = new ProducerRecord<>
                ("test", "name", "ForgetResult");

        // 发送消息的过程中可能会有序列化异常 分区器执行异常 或者发送到缓冲区异常
        // 发后即忘的方式不关注这些异常
        producer.send(record);

        producer.close();
    }

    /**
     * 同步发送 最慢
     */
    private static void sendMessageSync() throws Exception {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test", "name", "Sync"
        );

        /**
         * 发送成功，返回RecordMetadata对象
         * 发送失败，抛出异常
         *
         * 异常分为可恢复异常和不可恢复异常
         * 可恢复异常：旧的leader副本所在机器宕机，正在选举旧的follower作为新的leader副本，选举完成后，即可恢复正常
         * 对于可恢复异常,生产者配置了重试次数后，会进行重新发送
         *
         * 不可恢复异常，进行重试后也不能成功发送
         * 比如 ：消息数据太大，Kafka不能发送
         *
         * */
        RecordMetadata result = producer.send(record).get();

        // 主题 test
        System.out.println(result.topic());
        // 分区 0
        System.out.println(result.partition());
        // 在分区中的偏移     2
        System.out.println(result.offset());

        producer.close();

    }


    /**
     * 异步发送 最常用
     */
    private static void sendMessageCallback() {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test1", "name", "CallBack"
        );

        producer.send(record, new MyProducerCallBack());

        /**
         * 这里多次发送消息到有三个分区的主题test1中，为了验证自定义分区器的作用
         * */
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                "test1", "name1", "CallBack"
        );

        producer.send(record1, new MyProducerCallBack());


        ProducerRecord<String, String> record2 = new ProducerRecord<>(
                "test1", "name1", "haha"
        );

        producer.send(record, new MyProducerCallBack());
        producer.close();
    }

    /**
     * 异步发送的回调类
     */
    private static class MyProducerCallBack implements Callback {


        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {

            if (exception != null) {
                exception.printStackTrace();
                return;
            } else {
                System.out.println(metadata.topic());
                System.out.println(metadata.partition());
                System.out.println(metadata.offset());

                System.out.println("Coming in MyProducerCallBack");
            }


        }
    }


    public static void main(String[] args) throws Exception {

//        sendMessageForgetResult();
//        sendMessageSync();
        sendMessageCallback();
    }
}
