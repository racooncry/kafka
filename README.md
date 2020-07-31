# kafka

# stream input
$  ./bin/kafka-console-producer.sh --broker-list 192.168.1.71:9092 --topic stream-input

# stream output
./bin/kafka-console-consumer.sh --bootstrap-server 192.168.1.71:9092 \
 --topic stream-output \
 --property print.key=true \
 --property print.value=true \
 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
 --from-beginning


# run application com.shenfeng.yxw.kafka.stream.StreamSample

Hello World yxw
Hello World project
Hello yxw project