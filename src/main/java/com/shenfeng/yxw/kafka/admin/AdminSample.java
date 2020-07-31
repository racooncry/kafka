package com.shenfeng.yxw.kafka.admin;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;

/**
 * @Author yangxw
 * @Date 27/7/2020 下午4:14
 * @Description
 * @Version 1.0
 */
public class AdminSample {

    public static final String TOPOC_NAME = "yxw-top2";
    public static final String BROKER_LIST = "192.168.1.71:9092";
    public static void main(String[] args) throws Exception {
//        AdminClient adminClient = AdminSample.adminClient();
//        System.out.println("adminClient: " + adminClient);
//
        // 创建topic实例
        // createTopic();

        // 删除
        // deleteTopic();

        // 打印topic
         topicLists();

        // 描述topic
       // describeTopic();


        // 修改config
//        alterConfig();

        // 查看config
//        describeConfig();

        // 增加partitions
        //   incrPartitions(2);
    }


    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * 创建topic
     */
    public static void createTopic() throws Exception {
        AdminClient adminClient = adminClient();
        // 副本因子
        Short rs = 1;
        NewTopic newTopic = new NewTopic(TOPOC_NAME, 1, rs);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        topics.all().get();
        System.out.println("CreateTopicsResult: " + JSON.toJSONString(topics));
    }


    /**
     * 删除topic
     */
    public static void deleteTopic() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPOC_NAME));
        deleteTopicsResult.all().get();
        System.out.println("DeleteTopic: " + JSON.toJSONString(deleteTopicsResult));
    }


    public static void print(Map<String, TopicDescription> stringTopicDescriptionMap) {
        //    Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.stream().forEach((entry) -> {
            System.out.println("name: " + entry.getKey() + " , desc: " + entry.getValue());
        });
    }

    /**
     * 描述topic
     * name: yxw-topic2 ,
     * desc: (name=yxw-topic2,
     * internal=false,
     * partitions=(partition=0,
     * leader=192.168.1.71:9092
     * (id: 0 rack: null), replicas=192.168.1.71:9092 (id: 0 rack: null), isr=192.168.1.71:9092 (id: 0 rack: null)))
     */
    public static void describeTopic() throws Exception {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPOC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        print(stringTopicDescriptionMap);
    }


    /**
     * 获取topic列表
     */
    public static void topicLists() throws Exception {
        AdminClient adminClient = adminClient();

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        // 是否查看internal选项
        listTopicsOptions.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        Set<String> names = listTopicsResult.names().get();
        // 打印names
        names.stream().forEach(System.out::println);


        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        KafkaFuture<Map<String, TopicListing>> mapKafkaFuture = listTopicsResult.namesToListings();
        // 打印topicListings
        topicListings.stream().forEach((topicListing ->
                System.out.println(topicListing.name())

        ));
    }

    /**
     * 配置信息
     * configResource:
     * ConfigResource(type=TOPIC, name='yxw-topic2') ,
     * config: Config(entries=[ConfigEntry(name=compression.type,
     * value=producer, source=DEFAULT_CONFIG,
     * isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=leader.replication.throttled.replicas, value=, s
     * ource=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=message.format.version, value=2.1-IV2, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     * ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
     *
     * @throws Exception
     */
    public static void describeConfig() throws Exception {
        AdminClient adminClient = adminClient();
        // 集群时会将
        // ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, TOPOC_NAME);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPOC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.entrySet().stream().forEach((configResourceConfigEntry -> {
            System.out.println("configResource: " + configResourceConfigEntry.getKey() + " , config: " + configResourceConfigEntry.getValue());
        }));
    }


    /**
     * 修改config配置
     *
     * @throws Exception
     */
    public static void alterConfig() throws Exception {
        AdminClient adminClient = adminClient();

        //           version 1
        Map<ConfigResource, Config> map = new HashMap<>();
        // 组织两个参数
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPOC_NAME);
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "true")));
        map.put(configResource, config);
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(map);
        alterConfigsResult.all().get();


        //           version 2
    }


    /**
     * 增加partitions 数量
     * server path: /tmp/kafka-logs    topic-0 topic-1
     * @param partitions
     * @throws Exception
     */
    public static void incrPartitions(int partitions) throws Exception {
        AdminClient adminClient = AdminSample.adminClient();
        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        map.put(TOPOC_NAME, newPartitions);
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(map);
        createPartitionsResult.all().get();

    }





}
