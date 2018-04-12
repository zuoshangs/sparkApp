package com.zuoshangs.spark;

import com.alibaba.fastjson.JSON;
import com.zuoshangs.spark.config.bean.JavaBeanConfig;
import com.zuoshangs.spark.config.prop.AppPropertiesConfig;
import com.zuoshangs.spark.domain.model.Attr;
import com.zuoshangs.spark.domain.model.AttrVal;
import com.zuoshangs.spark.domain.model.HbaseNameAuthLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.hbase.HbaseConfigurationFactoryBean;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import scala.Tuple2;

import java.text.ParseException;
import java.util.*;

/**
 * Created by weike on 2018/4/11.
 */
public class LabelMakerApplication {
    public static void main(String[] args) throws ParseException {
        if(args.length<3){
            System.err.println("Usage: LabelMakerApplication <startTime> <endTime> <attrJson> ");
            System.err.println("./bin/spark-submit --master spark://10.40.3.236:7077 --class com.zuoshangs.spark.LabelMakerApplication /root/code/sparkApp/target/spark-app-1.0-SNAPSHOT-jar-with-dependencies.jar 20180410000000 20180411000000 {\"attrValList\":[{\"id\":1,\"name\":\"男\",\"rule\":\"{\\\"eq\\\":\\\"男\\\"}\"},{\"id\":1,\"name\":\"女\",\"rule\":\"{\\\"eq\\\":\\\"女\\\"}\"}],\"id\":1,\"name\":\"性别\"}");
            System.exit(1);
        }
        String pattern = "yyyyMMddHHmmss";
        Attr attr = JSON.parseObject(args[2],Attr.class);
        AppPropertiesConfig appPropertiesConfig = new AppPropertiesConfig();
        appPropertiesConfig.setEnableHbaseCache(true);
        appPropertiesConfig.setHbaseCacheSize(500);
        appPropertiesConfig.setHadoopDefaultFs("hdfs://10.40.3.236:9000");
        appPropertiesConfig.setHbaseZkHost("10.40.3.236");
        appPropertiesConfig.setHbaseZkPort("2181");
        appPropertiesConfig.setHbaseZkZnodeParent("/hbase");


        JavaBeanConfig javaBeanConfig = new JavaBeanConfig();
        org.apache.hadoop.conf.Configuration hadoopConfigurationConfigurer = javaBeanConfig.hadoopConfigurationConfigurer(appPropertiesConfig);

        HbaseConfigurationFactoryBean hbaseConfigurationFactoryBean = javaBeanConfig.hbaseConfigurationFactoryBeanConfigurer(hadoopConfigurationConfigurer,appPropertiesConfig);
        HbaseTemplate hbaseTemplate = javaBeanConfig.hbaseTemplateConfigurer(hbaseConfigurationFactoryBean);

       /* UserAuthByDateRepository userAuthByDateRepository = new UserAuthByDateRepository();
        userAuthByDateRepository.setConfig(appPropertiesConfig);
        userAuthByDateRepository.setHbaseTemplate(hbaseTemplate);



        Date startTime = DateUtils.parseDate(args[0],pattern);
        Date endTime = DateUtils.parseDate(args[1],pattern);
        List<HbaseNameAuthLog> list = userAuthByDateRepository.queryByRowKey(startTime,endTime);

        for (HbaseNameAuthLog hbaseNameAuthLog : list) {
            Integer attrValIdMatch = null;
            for (AttrVal attrVal : attr.getAttrValList()) {
                if(StringUtils.equals(attrVal.getName(),hbaseNameAuthLog.getGender())){
                    attrValIdMatch = attrVal.getId();
                    break;
                }
            }
            System.out.println("给用户"+hbaseNameAuthLog.getUserId()+",打标签:"+attr.getId()+",值："+attrValIdMatch);
        }*/

    }

}
