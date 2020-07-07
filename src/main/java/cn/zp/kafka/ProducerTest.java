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
        //����Broker��ַ ��ȡԪ����
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        // value�����л���  ���л����Զ���Э��Ľṹ
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // key�����л���  ���л����Զ���Э��Ľṹ
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //  acks 0 ���ͳ�ȥֱ�ӷ��� ������Brokerд����
        //  1  leaderд�ɹ��ͷ���
        //  all  ISR�б�ȫ��д�ɹ�CIA����
        properties.put("acks","all" );
        //���Դ���
        properties.put("retries",3 );
        //  sender�� batch �ﵽ���ŷ���
        properties.put("batch.size",323840 );
        // ���û�ﵽbatch size   �ﵽ����ʱ����뷢��
        properties.put("linger.ms", 10);
        //��������С
        properties.put("buffer.memory", 33554432);

        properties.put("max.block.ms", 3000);


        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> send = new ProducerRecord<String, String>("topicA", "key", "value");
         //�첽������Ϣ
        producer.send(send, new Callback() {

            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {

                    //��Ϣ���ͳɹ�
                } else {
                    //��Ϣ����ʧ��
                }

            }
        });

        //ͬ��������Ϣ
        producer.send(send).get();

        producer.close();
    }
}