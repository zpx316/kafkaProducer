package cn.zp.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author zp
 * @Description:
 * @date 2020-07-08 10:15
 */
public class MyPartitioner implements Partitioner {


    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

//        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic("order-topic");
//         //获取一共有几个partition
//        int num = partitionInfos.size();
        int a  = (Integer) key;

        if (a>0) {
            return 1;
        }
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}