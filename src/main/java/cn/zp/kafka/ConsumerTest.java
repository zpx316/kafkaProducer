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
                //ÿ1000ms��ȡһ�� ��Ϣ
                ConsumerRecords<String, String> records = consumer.poll(1000); // ��ʱʱ��
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
            //��������
            props.put("group.id", "groupId");
            //�Զ��ύoffset
            props.put("enable.auto.commit", "true");
            //�ύ���
            props.put("auto.commit.ineterval.ms", "1000");
            //��� Offset ��Χ���Դ�����Ŀ�ʼ����
            props.put("auto.offset.reset", "earliest");
            //key���л���
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            //value���л���
            props.put("value.deserializer", "org.apache.kafka.common.serialization.SttringDeserializer");
            //�������
            props.put("heartbeat.interval.ms", 1000);
            //�೤ʱ�䲻�ύ�������߳�
            props.put("session.timeout.ms", 10 * 1000);
            //���poll����ʱ����  ���������ʱ��ͻᱻ�߳�
            props.put("max.poll.interval.ms", 30 * 1000);
            //ÿ������Ϣ���������������ȡֵ
            props.put("fetch.max.bytes", 10485760);
            //ÿ�������ȥ������
            props.put("max.poll.records", 500);
            //��Զ�������socket
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