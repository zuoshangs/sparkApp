package com.zuoshangs.spark;



import com.zuoshangs.spark.extension.util.AESUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;


/**
 * User: hadoop
 * Date: 2014/10/10 0010
 * Time: 19:26
 */
public final class ZhugeIoSparkBatchApplication {
	private static final String SPACE = "\\t";
	private static final String APP_USER_ID_COLUMN = "app_user_id";

	public static void main(String[] args) throws Exception {
		if(args.length<2){
			System.err.println("Usage: ZhugeIoSparkBatchApplication <eventFileName> <userAttrFileName> ");
			System.err.println("./bin/spark-submit --master spark://10.40.3.236:7077 --class com.xs.micro.data.spark.ZhugeIoSparkBatchApplication /root/code/sparkApp/target/spark-app-1.0-SNAPSHOT.jar hdfs://10.40.3.236:9000/zhugeio/b_user_event_all_47271_000 hdfs://10.40.3.236:9000/zhugeio/b_user_property_47271_000");
			System.exit(1);
		}
		String eventFile = args[0];
		String userAttrFile = args[1];
		SparkConf sparkConf = new SparkConf().setAppName("ZhugeIoSparkBatchApplication");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> eventList = ctx.textFile(eventFile, 1);
		JavaRDD<String> userAttrList = ctx.textFile(userAttrFile, 1);

		JavaRDD<String> eventListAfterFilter = eventList.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] column = s.split(SPACE);
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
				//第2个是用户id
				return new Tuple2<String, String>(s.split(SPACE)[2], s);
			}
		});
		System.out.println("userAttr begin---------------------------------------");
		JavaRDD<String> userAttrListAfterFilter = userAttrList.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] column = s.split(SPACE);
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
				String[] arr = s.split(SPACE);
				String userIdEncode = arr[5];
				String userId = null;
				try {
					userId = AESUtil.decrypt(userIdEncode);
					arr[5] = userId;
					//TODO 这里需要处理
				} catch (AESUtil.AESFailedException e) {
					e.printStackTrace();
				}
				return new Tuple2<String, String>(arr[1], s);
			}
		});
		System.out.println(" ****************** join *******************");

		JavaPairRDD<String, Tuple2<String, String>> joinRDD = eventRdd.join(userAttrRdd);
		for (Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2 : joinRDD.collect()) {
			System.out.println("55555555555555555555555:"+stringTuple2Tuple2._1()+","+stringTuple2Tuple2._2()._1()+":"+stringTuple2Tuple2._2()._2());
		}
		 /*
        *   leftOuterJoin
        * */
		System.out.println(" ****************** leftOuterJoin *******************");
		JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoinRdd = eventRdd.leftOuterJoin(userAttrRdd);
		Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> it2 = leftOuterJoinRdd.collect().iterator();
		while (it2.hasNext()) {
			Tuple2<String, Tuple2<String, Optional<String>>> item = it2.next();
			System.out.println("key:" + item._1 + ", v1:" + item._2._1 + ", v2:" + item._2._2 );
		}
		ctx.stop();
	}
}