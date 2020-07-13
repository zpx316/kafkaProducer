package cn.zp.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zp
 * @Description:
 * @date 2020-07-08 10:32
 */
public class MyInteceptor  implements ProducerInterceptor {

    //消息发送之前
    public ProducerRecord onSend(ProducerRecord record) {

        System.out.println("消息的Key是："+record.key());
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}