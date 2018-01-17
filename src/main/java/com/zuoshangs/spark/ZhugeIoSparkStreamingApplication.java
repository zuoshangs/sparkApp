package com.zuoshangs.spark;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.alibaba.fastjson.JSON;

import scala.Tuple2;

public class ZhugeIoSparkStreamingApplication {
    public static void main(String[] args) throws InterruptedException {
        String brokers;
        String topics;
        String group;
        if(args.length<3){
//			brokers = "10.40.3.236:9092,10.40.3.238:9092,10.40.3.239:9092";
//			topics = "accesslog";
//			group = "toHbase1";
            //10.40.3.236:9092,10.40.3.238:9092,10.40.3.239:9092 accesslog toHbase1
            System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics>");
            System.err.println("./bin/spark-submit --master spark://10.40.3.236:7077 --class com.xs.micro.data.spark.ZhugeIoSparkStreamingApplication /root/code/sparkApp/target/spark-app-1.0-SNAPSHOT.jar 10.40.3.236:9092,10.40.3.238:9092,10.40.3.239:9092 accesslog toHbase1");
            System.exit(1);
        }
        brokers = args[0];
        topics = args[1];
        group = args[2];
        System.out.println("brokers:"+brokers+",topics:"+topics+",group:"+group);
        SparkConf conf = new SparkConf().setMaster("spark://10.40.3.236:7077").setAppName("ZhugeIoSparkStreamingHandler");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        sc.setLogLevel("WARN");
        sc.setCheckpointDir("hdfs://10.40.3.236:9000/zhuge_io_checkpoint");
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", group);
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("accesslog", 0), 2L);
        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<String,Object>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets)
        );
        JavaDStream<ConsumerRecord<String, Object>> filerDs = lines.filter(new Function<ConsumerRecord<String,Object>, Boolean>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(ConsumerRecord<String, Object> v1) throws Exception {
                try {
                    JSON.parseObject(v1.value().toString(), Map.class);
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        });

        JavaDStream<String> lineDs = filerDs.map(new Function<ConsumerRecord<String,Object>, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(ConsumerRecord<String, Object> v1) throws Exception {
                Map map = JSON.parseObject(v1.value().toString(), Map.class);
                return (String)map.get("businessType");
            }
        });

        JavaPairDStream<String, Integer> pairRdd = lineDs.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> updateStateByKeyRdd = pairRdd.updateStateByKey(
                // 这里的Optional,相当于scala中的样例类,就是Option,可以理解它代表一个状态,可能之前存在,也可能之前不存在
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>(){

                    private static final long serialVersionUID = 1L;

                    // 实际上,对于每个单词,每次batch计算的时候,都会调用这个函数,第一个参数,values相当于这个batch中,这个key的新的值,
                    // 可能有多个,比如一个hello,可能有2个1,(hello,1) (hello,1) 那么传入的是(1,1)
                    // 那么第二个参数表示的是这个key之前的状态,其实泛型的参数是你自己指定的
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        // 定义一个全局的计数
                        Integer newValue = 0;
                        if(state.isPresent()){
                            newValue = state.get();
                        }
                        for(Integer value : values){
                            newValue += value;
                        }
                        return Optional.of(newValue);
                    }

                });
        updateStateByKeyRdd.print();

        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }

}