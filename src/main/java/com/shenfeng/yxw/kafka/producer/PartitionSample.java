package com.shenfeng.yxw.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author yangxw
 * @Date 28/7/2020 上午9:17
 * @Description
 * @Version 1.0
 */
public class PartitionSample implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        /*
        key-1
        key-2
        key-3
         */
        String key = (String) o;
        String keyInt = key.substring(4);
        System.out.println("Keystr: " + key + ",KeyInt: " + keyInt);
        int i = Integer.parseInt(keyInt);
        return i % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
