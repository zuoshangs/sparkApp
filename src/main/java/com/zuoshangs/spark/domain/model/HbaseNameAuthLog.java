package com.zuoshangs.spark.domain.model;


/**
 * 实名认证记录
 * 
 * @author YuanZhiQiang
 *
 */
public class HbaseNameAuthLog{

	private String userId;
	/**
	 * 实名认证姓名
	 */
	private String name;

	/**
	 * 身份证号码
	 */
	private String idCard;

	/**
	 * 性别：男，女
	 */
	private String gender;

	/**
	 * 生日，格式：yyyy-MM-dd
	 */
	private String birthday;

	/**
	 * 省份，通过身份证号码获取
	 */
	private String province;

	/**
	 * 城市，通过身份证号码获取
	 */
	private String city;

	/**
	 * 实名认证时间，格式：yyyy-MM-dd HH:mm:ss
	 */
	private String time;

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIdCard() {
		return idCard;
	}

	public void setIdCard(String idCard) {
		this.idCard = idCard;
	}

	public String getBirthday() {
		return birthday;
	}

	public void setBirthday(String birthday) {
		this.birthday = birthday;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	@Override
	public String toString() {
		return "HbaseNameAuthLog{" +
				"name='" + name + '\'' +
				", idCard='" + idCard + '\'' +
				", gender='" + gender + '\'' +
				", birthday='" + birthday + '\'' +
				", province='" + province + '\'' +
				", city='" + city + '\'' +
				", time='" + time + '\'' +
				'}';
	}
}
