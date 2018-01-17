package com.zuoshangs.spark.domain;

import java.util.*;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyKafkaProducer {
     
    public void send(String broker,String topic,String content) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        //The "all" setting we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
        //“所有”设置将导致记录的完整提交阻塞，最慢的，但最持久的设置。
        props.put("acks", "all");
        //如果请求失败，生产者也会自动重试，即使设置成０ the producer can automatically retry.
        props.put("retries", 0);

        //The producer maintains buffers of unsent records for each partition.
        props.put("batch.size", 16384);
        //默认立即发送，这里这是延时毫秒数
        props.put("linger.ms", 1);
        //生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
        props.put("buffer.memory", 33554432);
        //The key.serializer and value.serializer instruct how to turn the key and value objects the user provides with their ProducerRecord into bytes.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka的生产者类
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //生产者的主要方法
        // close();//Close this producer.
        //   close(long timeout, TimeUnit timeUnit); //This method waits up to timeout for the producer to complete the sending of all incomplete requests.
        //  flush() ;所有缓存记录被立刻发送
            //这里平均写入４个分区
        producer.send(new ProducerRecord<>(topic,  content));
        producer.close();
    }
    public void sendBatch(String broker,String topicName, String businessType, List<Map<String, Object>> mapList, Date startDate, int pageNo) {
        List<List<Map<String, Object>>> paging = paging(mapList, 1000);// 分页
        if (CollectionUtils.isNotEmpty(paging)) {
            for (List<Map<String, Object>> list : paging) {
                Map<String, Object> sendMap = new HashMap<>();
                sendMap.put("businessType", businessType);
                sendMap.put("data", list);
                sendMap.put("startDate", startDate != null ? DateFormatUtils.format(startDate, "yyyy-MM-dd HH:mm") : "");
                sendMap.put("pageNo", pageNo);
                String json = JSON.toJSONString(sendMap);
                send(broker,topicName, json);
                System.out.println("collect send kafka topicName:[{"+topicName+"}], businessType:[{"+businessType+"}], data.size:[{CollectionUtils.size("+list+")}]");
            }
        }
    }
    protected List<List<Map<String, Object>>> paging(List<Map<String, Object>> list, int pageSize) {
        int totalCount = list.size();
        int pageCount;
        int m = totalCount % pageSize;
        if (m > 0) {
            pageCount = totalCount / pageSize + 1;
        } else {
            pageCount = totalCount / pageSize;
        }
        List<List<Map<String, Object>>> totalList = new ArrayList<List<Map<String, Object>>>();
        for (int i = 1; i <= pageCount; i++) {
            if (m == 0) {
                List<Map<String, Object>> subList = list.subList((i - 1) * pageSize, pageSize * (i));
                totalList.add(subList);
            } else {
                if (i == pageCount) {
                    List<Map<String, Object>> subList = list.subList((i - 1) * pageSize, totalCount);
                    totalList.add(subList);
                } else {
                    List<Map<String, Object>> subList = list.subList((i - 1) * pageSize, pageSize * i);
                    totalList.add(subList);
                }
            }
        }
        return totalList;
    }

    public static void main(String[] args) {
        MyKafkaProducer p = new MyKafkaProducer();
        p.send("10.40.3.236:9092,10.40.3.238:9092,10.40.3.239:9092","COLLECT_BATCH","{'businessType':'aaa'}");
    }

}