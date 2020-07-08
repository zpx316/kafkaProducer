package cn.zp.kafka;

import org.apache.kafka.clients.producer.*;
import org.json.JSONObject;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @author zp
 * @Description:
 * @date 2020-07-07 10:42
 */
public class ProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer<String, String> producer = createProducer();

        JSONObject order = createOrder();

        ProducerRecord<String, String> send = new ProducerRecord<String, String>("topicA", order.getString("orderId"), order.toString());
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


    private static JSONObject createOrder() {
        JSONObject order = new JSONObject();
        order.put("orderId", 63988);
        order.put("orderNo", UUID.randomUUID().toString());
        order.put("userId", 1147);
        order.put("productId", 380);
        order.put("purchaseCount", 2);
        order.put("productPrice", 50.0);
        order.put("totalAmount", 100.0);
        order.put("_OPERATION_", "PAY");
        return order;
    }


    public static Producer<String,String> createProducer(){
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
        return producer;

    }
}