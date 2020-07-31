package com.shenfeng.yxw.kafka.stream;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * @Author yangxw
 * @Date 31/7/2020 上午8:57
 * @Description
 * @Version 1.0
 */
public class StreamSample {

    public static final String INPUT_TOPIC = "stream-input";
    public static final String OUTPUT_TOPIC = "stream-output";
    public static Properties properties;
    public static final String BROKER_LIST = "192.168.1.71:9092";

    static {
        properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    }


    public static void main(String[] args) {

        // 如何构建流结构处理
        final StreamsBuilder builder = new StreamsBuilder();
        // 构建Wordcount process
        // wordcountStream(builder);
        forEachStream(builder);
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();
    }

    // 如何定义流记算过程
    static void wordcountStream(final StreamsBuilder builder) {
        // 不断从INPUT_TOPIC上获取新数据，并且追加到流上的一个抽象对象
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        // KTable时数据集合的抽象对象
        // 算子
        final KTable<String, Long> count =
                // flatMapValue 数据拆分->将一行数据拆分为多行数据，key1,value hello World
                /*
                key 1 , value hello   -> Hello 1 World 2
                key 2 , value World
                 */
                source.flatMapValues(value ->
                        Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                        // 合并-> 按value值合并
                        .groupBy((key, value) -> value)
                        // 统计出现的总数
                        .count();
        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }


    // 如何定义流记算过程
    static void forEachStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        source.flatMapValues(value ->
                Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .foreach((key, value) -> System.out.println(key + ": " + value));
    }
}
