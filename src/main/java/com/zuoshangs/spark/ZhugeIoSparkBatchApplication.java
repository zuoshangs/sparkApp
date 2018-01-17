package com.zuoshangs.spark;


import com.zuoshangs.spark.domain.MyKafkaProducer;
import com.zuoshangs.spark.extension.util.AESUtil;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * User: hadoop
 * Date: 2014/10/10 0010
 * Time: 19:26
 */
public final class ZhugeIoSparkBatchApplication {
	private static final String TAB = "\\t";
	private static final String APP_USER_ID_COLUMN = "app_user_id";
	private static final String BUSINESS_TYPE = "CollectEvent";
	private static final int PAGE_NO = 1000;

	public static void main(String[] args) throws Exception {
		if(args.length<4){
			System.err.println("Usage: ZhugeIoSparkBatchApplication <eventFileName> <userAttrFileName> <brokers> <topic> ");
			System.err.println("./bin/spark-submit --master spark://10.40.3.236:7077 --class com.zuoshangs.spark.ZhugeIoSparkBatchApplication /root/code/sparkApp/target/spark-app-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.40.3.236:9000/zhugeio/b_user_event_all_47271_000 hdfs://10.40.3.236:9000/zhugeio/b_user_property_47271_000 10.40.3.236:9092,10.40.3.238:9092,10.40.3.239:9092 COLLECT_BATCH");
			System.exit(1);
		}
		String eventFile = args[0];
		String userAttrFile = args[1];
		String brokers = args[2];
		String topics = args[3];
		SparkConf sparkConf = new SparkConf().setAppName("ZhugeIoSparkBatchApplication");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> eventList = ctx.textFile(eventFile, 1);
		JavaRDD<String> userAttrList = ctx.textFile(userAttrFile, 1);

		JavaRDD<String> eventListAfterFilter = eventList.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] column = s.split(TAB);
				//第2个是用户id
				if("138291".equals(column[2])){
					return true;
				}
				return false;
			}
		});

		JavaPairRDD<String, String> eventRdd = eventListAfterFilter.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				String[] arr = s.split(TAB);
				String outerUserId = arr[2];
				/**
				 * 1:deviceId
				 * 2:用户id
				 * 4：eventName
				 * 5：eventId
				 * 7：时间
				 * 8：day
				 */
				//第2个是用户id
				return new Tuple2<>(outerUserId, String.join("\t",arr[1],arr[2],arr[4],arr[5],arr[7],arr[8]));
			}
		});
		System.out.println("userAttr begin---------------------------------------");
		JavaRDD<String> userAttrListAfterFilter = userAttrList.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] column = s.split(TAB);
				//第三个是属性名，属性名等于app_user_id表示用户id
				if(APP_USER_ID_COLUMN.equals(column[3])){
					return true;
				}
				return false;
			}
		});

		JavaPairRDD<String, String> userAttrRdd = userAttrListAfterFilter.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				//第5个是用户id加密后的值
				String[] arr = s.split(TAB);
				String userIdEncode = arr[5];
				String outerUserId = arr[1];
				String bizUserId = null;
				try {
					bizUserId = AESUtil.decrypt(userIdEncode);
					//TODO 这里需要处理
				} catch (AESUtil.AESFailedException e) {
					e.printStackTrace();
				}
				//第1个是诸葛IO的userId
				return new Tuple2<>(outerUserId, String.join("\t",outerUserId,userIdEncode,bizUserId));
			}
		});

		 /*
        *   leftOuterJoin
        * */
		MyKafkaProducer myKafkaProducer = new MyKafkaProducer();
		System.out.println(" ****************** leftOuterJoin *******************");
		JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoinRdd = eventRdd.leftOuterJoin(userAttrRdd);
		Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> it2 = leftOuterJoinRdd.collect().iterator();
		List<Map<String, Object>> sendList = new ArrayList<>();
		while (it2.hasNext()) {
			Tuple2<String, Tuple2<String, Optional<String>>> item = it2.next();
			String[] arr = item._2._1.split(TAB);
			/*UserEvent userEvent = new UserEvent();
			userEvent.setDeviceId(arr[0]);
			userEvent.setOuterUserId(arr[1]);
			userEvent.setEventName(arr[2]);
			userEvent.setEventId(arr[3]);
			userEvent.setEventTime(arr[4]);
			userEvent.setEventDay(arr[5]);*/
			String bizUserId = item._2._2.get().split(TAB)[2];
			//userEvent.setBizUserId(bizUserId);
			Map<String, Object> map = new HashMap();
			map.put("deviceId",arr[0]);
			map.put("outerUserId",arr[1]);
			map.put("eventName",arr[2]);
			map.put("eventId",arr[3]);
			map.put("eventTime",arr[4]);
			map.put("eventDay",arr[5]);
			map.put("bizUserId",bizUserId);
			System.out.println("userEvent:" + map );
			sendList.add(map);
		}
		myKafkaProducer.sendBatch(brokers,topics,BUSINESS_TYPE,sendList,new Date(),PAGE_NO);
		ctx.stop();
	}
}