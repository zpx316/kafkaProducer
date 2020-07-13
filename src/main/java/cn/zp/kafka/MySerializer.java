package cn.zp.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author zp
 * @Description:
 * @date 2020-
 * 07-08 10:28
 */
public class MySerializer  implements Serializer<String> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topic, String data) {

        byte[] bytes = data.getBytes();
        return bytes;
    }

    public void close() {

    }
}