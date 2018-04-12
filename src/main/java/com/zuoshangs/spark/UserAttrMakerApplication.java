package com.zuoshangs.spark;

import com.zuoshangs.spark.config.bean.JavaBeanConfig;
import com.zuoshangs.spark.config.prop.AppPropertiesConfig;
import com.zuoshangs.spark.extension.util.HbaseUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by weike on 2018/4/12.
 */
public class UserAttrMakerApplication {
    public static void main(String[] args) throws ParseException {
        if(args.length<3){
            System.err.println("Usage: UserAttrMakerApplication <startTime> <endTime> <attrJson> ");
            System.err.println("./bin/spark-submit --master spark://10.40.3.236:7077 --class com.zuoshangs.spark.UserAttrMakerApplication /root/code/sparkApp/target/spark-app-1.0-SNAPSHOT-jar-with-dependencies.jar 20180410000000 20180411000000 {\"attrValList\":[{\"id\":1,\"name\":\"男\",\"rule\":\"{\\\"eq\\\":\\\"男\\\"}\"},{\"id\":1,\"name\":\"女\",\"rule\":\"{\\\"eq\\\":\\\"女\\\"}\"}],\"id\":1,\"name\":\"性别\"}");
            System.exit(1);
        }
        String pattern = "yyyyMMddHHmmss";
        Date startTime = DateUtils.parseDate(args[0],pattern);
        Date endTime = DateUtils.parseDate(args[1],pattern);
        System.out.println("start:"+startTime+",end:"+endTime);

        SparkConf sparkConf = new SparkConf().setAppName("UserAttrMakerApplication");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        AppPropertiesConfig appPropertiesConfig = new AppPropertiesConfig();
        appPropertiesConfig.setEnableHbaseCache(true);
        appPropertiesConfig.setHbaseCacheSize(500);
        appPropertiesConfig.setHadoopDefaultFs("hdfs://10.40.3.236:9000");
        appPropertiesConfig.setHbaseZkHost("10.40.3.236");
        appPropertiesConfig.setHbaseZkPort("2181");
        appPropertiesConfig.setHbaseZkZnodeParent("/hbase");

        JavaBeanConfig javaBeanConfig = new JavaBeanConfig();
        org.apache.hadoop.conf.Configuration hadoopConfig = javaBeanConfig.hadoopConfigurationConfigurer(appPropertiesConfig);


        String startRow = HbaseUtil.queryStartRowByDate(startTime);
        String stopRow = HbaseUtil.queryEndRowByDate(endTime);
        System.out.println("-------------------------startRow:"+startRow+",stopRow:"+stopRow);

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));

        try {
            String tableName = "USER_AUTH_BY_DATE";
            hadoopConfig.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            hadoopConfig.set(TableInputFormat.SCAN, ScanToString);

            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(hadoopConfig, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            System.out.println("----------55555xxxxxxxxxxxxxxxxxxxxxxxxxxx-------"+myRDD.count());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
