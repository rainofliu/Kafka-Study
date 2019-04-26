package com.ajin.kafkastudy;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * @Author: ajin
 * @Date: 2019/4/26 09:40
 * <p>
 * 自定义的分区器
 */

public class CustomPartitioner implements Partitioner {

    private static final String NAME = "name";

    /**
     * 消息中包含key，我们就根据key计算出一个分区
     * 消息中不包含key（key==null)，我们就轮询，把消息发给分区
     * （相同key的消息，会被发送到同一个分区中）
     *
     * 消息的key和分区要保持一个不变的映射关系，默认的分区器不能满足这样的要求，
     * 所以我们需要自己自定义分区器
     * 如果新增分区，默认分区器不能保证这个映射关系；
     * 如果某个分区所在机器宕机了，默认分区器会将key对应的消息发送到该分区上，这就麻烦了
     * */

    /**
     * 分区
     *
     * @param cluster kafka集群
     */
    @Override
    public int partition(String topic,
                         Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes,
                         Cluster cluster) {
        // 从Kafka集群中获取某一个Topic下的分区信息
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        // 获取分区的个数
        int numPartitions = partitionInfos.size();

        if (null == keyBytes || !(key instanceof String)) {
            throw new InvalidRecordException("kafka message must have key");
        }

        if (numPartitions == 1) {
            return 0;
        }

        if (NAME.equals(key)) {
            return numPartitions - 1;
        }

        return Math.abs(Utils.murmur2(keyBytes) % (numPartitions - 1));
    }

    /**
     * 关闭分区器
     */
    @Override
    public void close() {

    }

    /**
     * 配置分区器
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
