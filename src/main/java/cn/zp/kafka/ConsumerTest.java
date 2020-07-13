package cn.zp.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zp
 * @Description:
 * @date 2020-07-08 10:50
 */
public class ConsumerTest {


    public static ExecutorService threadPool = Executors.newFixedThreadPool(20);

    public static void main(String[] args) {
        String topicName = "test-topic";

        KafkaConsumer<String, String> consumer = createConsumer();

        consumer.subscribe(Arrays.asList(topicName));
        try {
            while(true) {
                //每1000ms拉取一次 消息
                ConsumerRecords<String, String> records = consumer.poll(1000); // 超时时间
                for(ConsumerRecord<String, String> record : records) {

                    threadPool.submit();
                    System.out.println(record.offset() + ", " + record.key() + ", " + record.value());
                }
            }
        } catch(Exception e) {

        }
        }


        public static KafkaConsumer<String,String> createConsumer(){


            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            //消费者组
            props.put("group.id", "groupId");
            //自动提交offset
            props.put("enable.auto.commit", "true");
            //提交间隔
            props.put("auto.commit.ineterval.ms", "1000");
            //如果 Offset 范围不对从最早的开始消费
            props.put("auto.offset.reset", "earliest");
            //key序列化器
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            //value序列化器
            props.put("value.deserializer", "org.apache.kafka.common.serialization.SttringDeserializer");
            //心跳间隔
            props.put("heartbeat.interval.ms", 1000);
            //多长时间不提交心跳会踢出
            props.put("session.timeout.ms", 10 * 1000);
            //最大poll操作时间间隔  ，超过这个时间就会被踢出
            props.put("max.poll.interval.ms", 30 * 1000);
            //每次拉消息（多条），最大拉取值
            props.put("fetch.max.bytes", 10485760);
            //每次最多拉去多少条
            props.put("max.poll.records", 500);
            //永远不会回收socket
            props.put("connection.max.idle.ms", -1);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

            return consumer;
        }


        static class ConsumerTask implements Runnable{

            private JSONObject value ;
            public ConsumerTask(JSONObject value) {

                this.value = value;
            }

            public void run() {
                System.out.println(value.toString());
            }
        }
}