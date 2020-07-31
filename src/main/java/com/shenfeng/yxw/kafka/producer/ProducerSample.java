package com.shenfeng.yxw.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @Author yangxw
 * @Date 28/7/2020 上午8:32
 * @Description
 * @Version 1.0
 */
public class ProducerSample {
    public static final String TOPOC_NAME = "yxw-topic2";
    public static final String BROKER_LIST = "192.168.1.71:9092";

    public static Properties properties;

    static {
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    }

    public static void main(String[] args) throws Exception {
        // producerSend异步演示
        // producerSend();

        // 异步阻塞发送
        // producerSendAsync();

        // 异步发送带回调
        // producerSendWithCallback();

        // 异步发送带回调和Partition负载均衡
        producerSendWithCallbackAndPartition();
    }


    /**
     * 异步发送,常用这种
     */
    public static void producerSend() {
        // Producer的主对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPOC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> send = producer.send(producerRecord);

        }


        // 所有的通道都需要关闭
        producer.close();
    }


    /**
     * Producer异步阻塞
     */
    public static void producerSendAsync() throws Exception {
        // Producer的主对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPOC_NAME, key, value);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.println("partition: " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
        }


        // 所有的通道都需要关闭
        producer.close();
    }


    /**
     * 异步发送带回调
     */
    public static void producerSendWithCallback() {
        // Producer的主对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPOC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> send = producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("partition: " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
                }
            });

        }


        // 所有的通道都需要关闭
        producer.close();
    }


    /**
     * 异步发送带回调,带分区
     */
    public static void producerSendWithCallbackAndPartition() {
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.shenfeng.yxw.kafka.producer.PartitionSample");
        // Producer的主对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPOC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> send = producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("partition: " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
                }
            });

        }


        // 所有的通道都需要关闭
        producer.close();
    }
}
