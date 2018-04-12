package com.zuoshangs.spark.config.prop;



/**
 * 
 * 应用配置类，应用级别的所有配置统一入口<br>
 * 不建议将配置进行分散管理，原则上一个项目中仅有一个@ConfigurationProperties注解文件。<br>
 * 并且建议应用级别配置均以app.conf开头，用以和框架配置做命名空间区分
 * 
 * @author YuanZhiQiang
 *
 */
public class AppPropertiesConfig {
	private String hadoopDefaultFs;
	private String hbaseZkHost;
	private String hbaseZkPort;
	private String hbaseZkZnodeParent;
	private int hbaseCacheSize;
	private boolean enableHbaseCache;
	private String hbaseNamespace;

	public String getHadoopDefaultFs() {
		return hadoopDefaultFs;
	}

	public void setHadoopDefaultFs(String hadoopDefaultFs) {
		this.hadoopDefaultFs = hadoopDefaultFs;
	}

	public String getHbaseZkHost() {
		return hbaseZkHost;
	}

	public void setHbaseZkHost(String hbaseZkHost) {
		this.hbaseZkHost = hbaseZkHost;
	}

	public String getHbaseZkPort() {
		return hbaseZkPort;
	}

	public void setHbaseZkPort(String hbaseZkPort) {
		this.hbaseZkPort = hbaseZkPort;
	}

	public String getHbaseZkZnodeParent() {
		return hbaseZkZnodeParent;
	}

	public void setHbaseZkZnodeParent(String hbaseZkZnodeParent) {
		this.hbaseZkZnodeParent = hbaseZkZnodeParent;
	}

	public int getHbaseCacheSize() {
		return hbaseCacheSize;
	}

	public void setHbaseCacheSize(int hbaseCacheSize) {
		this.hbaseCacheSize = hbaseCacheSize;
	}

	public boolean isEnableHbaseCache() {
		return enableHbaseCache;
	}

	public void setEnableHbaseCache(boolean enableHbaseCache) {
		this.enableHbaseCache = enableHbaseCache;
	}

	public String getHbaseNamespace() {
		return hbaseNamespace;
	}

	public void setHbaseNamespace(String hbaseNamespace) {
		this.hbaseNamespace = hbaseNamespace;
	}
}
