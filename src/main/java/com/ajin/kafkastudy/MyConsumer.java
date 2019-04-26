package com.ajin.kafkastudy;

import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author: ajin
 * @Date: 2019/4/26 10:04
 */

public class MyConsumer {

    private static KafkaConsumer<String, String> consumer;

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
                "org.apache.kafka.common.serialization.StringDeserializer");
        // group.id 设置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaStudy");
    }

    /**
     * 自动提交位移
     * <p>
     * 自动提交位移有一个问题，就是比如我consumer.poll()拉取消息，还没做业务逻辑的处理，
     * 消费者客户端宕机了，再重启，再去消费，就会从下一条数据开始消费
     * </p>
     */
    @SuppressWarnings("all")
    private static void generalConsumeMessageAutoCommit() {

        // 设置自动提交位移 enable.auto.commit=true
        // 消费者会在consumer.poll(100)执行后，每隔5秒去提交位移
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 创建消费者
        consumer = new KafkaConsumer<String, String>(properties);
        // 订阅topic test1 该主题有三个分区
        consumer.subscribe(Collections.singleton("test1"));

        try {
            while (true) {

                boolean flag = true;
                // 获取消息列表
                ConsumerRecords<String, String> records = consumer.poll(100);

                // 遍历消息列表
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息详细信息
                    System.out.printf(
                            "topic = %s,partition = %s,key = %s,value = %s\n",
                            record.topic(), record.partition(),
                            record.key(), record.value()
                    );
//                    // 消息全部接收后，会收到“done”
//                    if (record.value().equals("done")) {
//                        flag = false;
//                    }

                }

                // 跳出while循环
                if (!flag) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

    /**
     * 手动提交当前位移（同步）
     */
    @SuppressWarnings("all")
    private static void generalConsumerMessageSyncCommit() {

        // 设置手动提交当前位移
        properties.put("auto.commit.offset", false);
        // 创建消费者
        consumer = new KafkaConsumer<String, String>(properties);
        // 消费者订阅Topic test1
        consumer.subscribe(Collections.singletonList("test1"));

        while (true) {

            boolean flag = true;

            ConsumerRecords<String, String> records =
                    consumer.poll(100);


            for (ConsumerRecord<String, String> record : records) {

                // 打印消息详细信息
                System.out.printf(
                        "topic = %s,partition = %s,key = %s,value = %s\n",
                        record.topic(), record.partition(),
                        record.key(), record.value()
                );


//                // 消息全部接收后，会收到“done”
//                if (record.value().equals("done")) {
//                    flag = false;
//                }

            }

            // 手动同步提交位移 会阻塞，但是它如果提交失败的话，是会主动重试的
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                System.out.println("commit failed error :" + e.getMessage());
            }

            if (!flag) {
                break;
            }
        }
    }

    /**
     * 手动异步提交当前位移
     */
    @SuppressWarnings("all")
    private static void generalConsumeMessageAsyncCommit() {

        // 设置手动提交当前位移
        properties.put("auto.commit.offset", false);
        // 创建消费者
        consumer = new KafkaConsumer<String, String>(properties);
        // 消费者订阅Topic test1
        consumer.subscribe(Collections.singletonList("test1"));

        while (true) {

            boolean flag = true;

            ConsumerRecords<String, String> records =
                    consumer.poll(100);


            for (ConsumerRecord<String, String> record : records) {

                // 打印消息详细信息
                System.out.printf(
                        "topic = %s,partition = %s,key = %s,value = %s\n",
                        record.topic(), record.partition(),
                        record.key(), record.value()
                );


//                // 消息全部接收后，会收到“done”
//                if (record.value().equals("done")) {
//                    flag = false;
//                }

            }

            // 手动异步提交位移，提交失败不会重试;如果进行重试，会导致消息重复消费（提交速度快）
            /**
             * 比如，A提交了offset为2000，提交失败；
             * B提交offset为3000，提交成功；
             * 现在进行A的重试，就会将offset重试为2000.那么就会从2000开始消费，就导致了消息的重复消费
             * */
            try {
                // commit A offset 2000
                // commit B offset 3000
                consumer.commitAsync();
            } catch (CommitFailedException e) {
                System.out.println("commit failed error :" + e.getMessage());
            }

            if (!flag) {
                break;
            }
        }

    }

    /**
     * 手动异步提交当前位移带回调
     */
    @SuppressWarnings("all")
    private static void generalConsumeMessageAsyncCommitWithCallback() {

        // 设置手动提交当前位移
        properties.put("auto.commit.offset", false);
        // 创建消费者
        consumer = new KafkaConsumer<String, String>(properties);
        // 消费者订阅Topic test1
        consumer.subscribe(Collections.singletonList("test1"));

        while (true) {

            boolean flag = true;

            ConsumerRecords<String, String> records =
                    consumer.poll(100);


            for (ConsumerRecord<String, String> record : records) {

                // 打印消息详细信息
                System.out.printf(
                        "topic = %s,partition = %s,key = %s,value = %s\n",
                        record.topic(), record.partition(),
                        record.key(), record.value()
                );


//                // 消息全部接收后，会收到“done”
//                if (record.value().equals("done")) {
//                    flag = false;
//                }

            }


            try {
                consumer.commitAsync(
                        // 位移提交的回调函数
                        (offsets, exception) -> {
                            if (exception != null) {
                                System.out.println("commit failed for offset :" +
                                        exception.getMessage());
                                // 如果想重试提交位移，可以在每一次设置一个值，
                                // 然后每接收一个批次的消息，就递增这个值，然后根据值判断：是否需要重试
                            }

                        }
                );
            } catch (CommitFailedException e) {
                System.out.println("commit failed error :" + e.getMessage());
            }

            if (!flag) {
                break;
            }
        }
    }

    /**
     * 混合同步异步提交位移
     */
    @SuppressWarnings("all")
    private static void mixSyncAndAsyncCommit() {


        // 设置手动提交当前位移
        properties.put("auto.commit.offset", false);
        // 创建消费者
        consumer = new KafkaConsumer<String, String>(properties);
        // 消费者订阅Topic test1
        consumer.subscribe(Collections.singletonList("test1"));


        try {

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {

                    // 打印消息详细信息
                    System.out.printf(
                            "topic = %s,partition = %s,key = %s,value = %s\n",
                            record.topic(), record.partition(),
                            record.key(), record.value()
                    );

                }
                // 异步提交
                consumer.commitAsync();


            }
        } catch (Exception ex) {
            System.out.println("commit async error :" + ex.getMessage());
        } finally {
            // 同步提交
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }


        }
    }


    public static void main(String[] args) {
//        generalConsumeMessageAutoCommit();
    }

}
