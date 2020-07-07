package cn.zp.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author zp
 * @Description:
 * @date 2020-07-07 10:42
 */
public class ProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //设置Broker地址 拉取元数据
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        // value的序列化器  序列化成自定义协议的结构
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // key的序列化器  序列化成自定义协议的结构
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //  acks 0 发送出去直接返回 ，不等Broker写数据
        //  1  leader写成功就返回
        //  all  ISR列表全部写成功CIA返回
        properties.put("acks","all" );
        //重试次数
        properties.put("retries",3 );
        //  sender的 batch 达到多大才发送
        properties.put("batch.size",323840 );
        // 如果没达到batch size   达到多少时间必须发送
        properties.put("linger.ms", 10);
        //缓冲区大小
        properties.put("buffer.memory", 33554432);

        properties.put("max.block.ms", 3000);


        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> send = new ProducerRecord<String, String>("topicA", "key", "value");
         //异步发送消息
        producer.send(send, new Callback() {

            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {

                    //消息发送成功
                } else {
                    //消息发送失败
                }

            }
        });

        //同步发送消息
        producer.send(send).get();

        producer.close();
    }
}