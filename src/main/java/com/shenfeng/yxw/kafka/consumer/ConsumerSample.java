package com.shenfeng.yxw.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @Author yangxw
 * @Date 29/7/2020 上午8:41
 * @Description
 * @Version 1.0
 */
public class ConsumerSample {
    public static final String TOPIC_NAME = "yxw-topic2";
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

    /*
    工作里有这种用法，但是不推荐
     */
    private static void helloworld() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费订阅topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition = %d , offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
        }

    }

    /*
     手动提交offset
       */
    private static void commitOffsetManaul() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费订阅topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                // 想把数据保存到数据库，成功就成功，不成功...
                // TODO: 存库
                System.out.printf("partition = %d , offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());

                // 如果失败，回滚,不要提交offset
            }

            //  手动offset提交
            consumer.commitAsync();
        }

    }

    /*
  手动提交offset,并且控制partition
    */
    private static void commitOffsetManaulWithPartition() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费订阅topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);

            // 每个partition单独处理
            // TODO: 多线程处理
            for (TopicPartition topicPartition :
                    records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(topicPartition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("current thread name = %s , patition = %d , offset = %d, key = %s, value = %s%n",
                            Thread.currentThread().getName(),
                            record.partition(), record.offset(), record.key(), record.value());
                }
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                //  手动offset提交
                consumer.commitSync(map);
                System.out.println("====================partition - " + topicPartition + "=====================");
            }
        }

    }


    /*
手动提交offset,并且控制partition,更高级
*/
    private static void commitOffsetManaulWithPartitionHigher() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        // 两个partition0,1
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);

        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);


        // 消费订阅topic
//        consumer.subscribe(Arrays.asList(TOPOC_NAME));
        // 消费订阅某个topic的分区
        consumer.assign(Arrays.asList(p0));


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);

            // 每个partition单独处理
            // TODO: 多线程处理
            for (TopicPartition topicPartition :
                    records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(topicPartition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("partition = %d , offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                //  手动offset提交
                consumer.commitSync(map);
                System.out.println("====================partition - " + topicPartition + "=====================");
            }
        }

    }


    /*
  手动提交offset,并且指定offset
  */
    private static void commitOffsetManaulWithPartitionAssignOffset() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        // 两个partition0,1
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);

        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);


        // 消费订阅topic
//        consumer.subscribe(Arrays.asList(TOPOC_NAME));
        // 消费订阅某个topic的分区
        consumer.assign(Arrays.asList(p0));


        while (true) {
            // 手动指定offset
            /*
            一、人为控制offset位置
            1、第一次从0消费【一般情况】
            2、比如一次消费了100条，offset置为101，下次消费的起始位置并且存入redis
            3、每次poll之前，从redis中获取最新的offset位置
            4、每次从这个位置开始消费

            二、失败了，重复消费
            记录失败的起始位置
             */
            consumer.seek(p0, 900);


            ConsumerRecords<String, String> records = consumer.poll(10000);

            // 每个partition单独处理
            // TODO: 多线程处理
            for (TopicPartition topicPartition :
                    records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(topicPartition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.err.printf("partition = %d , offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                //  手动offset提交
                consumer.commitSync(map);
                System.out.println("====================partition - " + topicPartition + "=====================");
            }
        }

    }


    /**
     * 限流
     */
    private static void rateLimit() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        // 两个partition0,1
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);

        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);


        // 消费订阅topic
//        consumer.subscribe(Arrays.asList(TOPOC_NAME));
        // 消费订阅某个topic的分区
        consumer.assign(Arrays.asList(p0, p1));


        while (true) {
            // 手动指定offset
            /*
            一、人为控制offset位置
            1、第一次从0消费【一般情况】
            2、比如一次消费了100条，offset置为101，下次消费的起始位置并且存入redis
            3、每次poll之前，从redis中获取最新的offset位置
            4、每次从这个位置开始消费

            二、失败了，重复消费
            记录失败的起始位置
             */
            //  consumer.seek(p0, 700);


            ConsumerRecords<String, String> records = consumer.poll(10000);
            long totalNum = 40;
            // 每个partition单独处理
            // TODO: 多线程处理
            for (TopicPartition topicPartition :
                    records.partitions()) {
                /*
                1、接收到record信息以后，去令牌桶中拿取令牌
                2、如果获取到令牌，则继续业务 处理
                3、如果获取不到令牌，则pause等待令牌
                4、当令牌桶中的令牌足够，则将consumer置为resume状态
                 */
                long num = 0;
                List<ConsumerRecord<String, String>> pRecord = records.records(topicPartition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("partition = %d , offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    num++;
                    System.out.println("num: " + num);
                    if (record.partition() == 0) {
                        if (num >= totalNum) {
                            System.out.println("============================暂停p0============================");
                            consumer.pause(Arrays.asList(p0));
                        }
                    }
                    if (record.partition() == 1) {

                        if (num == totalNum) {
                            System.out.println("============================恢复p0============================");
                            consumer.resume(Arrays.asList(p0));
                        }
                    }
                }


                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                //  手动offset提交
                consumer.commitSync(map);
                System.out.println("====================partition - " + topicPartition + "=====================");
            }
        }

    }

    public static void main(String[] args) {
        // 自动提交
        // helloworld();

        // 手动提交offset
        // commitOffsetManaul();

        // 手动对每个partition进行提交
        // commitOffsetManaulWithPartition();

        // 手动订阅某个分区 并提交offset
        // commitOffsetManaulWithPartitionHigher();

        // 指定offset
        commitOffsetManaulWithPartitionAssignOffset();

        // 限流
        // rateLimit();
    }
}
