package com.zuoshangs.spark.config.bean;

import com.zuoshangs.spark.config.prop.AppPropertiesConfig;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.data.hadoop.hbase.HbaseConfigurationFactoryBean;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import java.util.Properties;

public class JavaBeanConfig {

	public HbaseConfigurationFactoryBean hbaseConfigurationFactoryBeanConfigurer(org.apache.hadoop.conf.Configuration hadoopConfiguration, AppPropertiesConfig appPropertiesConfig){
		HbaseConfigurationFactoryBean hbaseConfigurationFactoryBean = new HbaseConfigurationFactoryBean();
		Properties properties = new Properties();
		properties.setProperty("hbase.zookeeper.quorum",appPropertiesConfig.getHbaseZkHost());
		properties.setProperty("hbase.zookeeper.property.clientPort",appPropertiesConfig.getHbaseZkPort());
		properties.setProperty("zookeeper.znode.parent",appPropertiesConfig.getHbaseZkZnodeParent());
		hbaseConfigurationFactoryBean.setProperties(properties);
		hbaseConfigurationFactoryBean.setConfiguration(hadoopConfiguration);
		hbaseConfigurationFactoryBean.afterPropertiesSet();
		return hbaseConfigurationFactoryBean;
	}

	public org.apache.hadoop.conf.Configuration hadoopConfigurationConfigurer(AppPropertiesConfig appPropertiesConfig){
		org.apache.hadoop.conf.Configuration hadoopConfig = HBaseConfiguration.create();
		hadoopConfig.set("fs.defaultFS",appPropertiesConfig.getHadoopDefaultFs());
		return hadoopConfig;
	}
	public HbaseTemplate hbaseTemplateConfigurer(HbaseConfigurationFactoryBean hbaseConfigurationFactoryBean){
		HbaseTemplate hbaseTemplate = new HbaseTemplate(hbaseConfigurationFactoryBean.getObject());
		return hbaseTemplate;
	}
}