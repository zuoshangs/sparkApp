package com.zuoshangs.spark.extension.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * Hbase工具类
 * 
 * @author YuanZhiQiang
 *
 */
@Service
public class HbaseUtil {

	// 1分(min)=60000毫秒(ms)
	public static final long MINUTE_TO_MILLISECOND = 60000L;
	// 1时(h)=3600000毫秒(ms)

	public static final long HOUR_TO_MILLISECOND = 3600000L;

	// 1天(d)=86400000毫秒(ms)
	public static final long DAY_TO_MILLISECOND = 86400000L;

	// 1周(week)=604800000毫秒(ms)
	public static final long WEEK_TO_MILLISECOND = 604800000L;

	// 15天(d)=1296000000毫秒(ms)
	public static final long DAY15__TO_MILLISECOND = 1296000000L;

	// 30天(d)=2592000000毫秒(ms)
	public static final long DAY30__TO_MILLISECOND = 2592000000L;

	// 1年(yr)=31536000000毫秒(ms)
	public static final long YEAR_TO_MILLISECOND = 31536000000L;

	// 按照用户ID迭代步长
	public static final int USER_STEP_NUM = 100;


	/**
	 * ROW KEY 定长补齐字符
	 */
	private static final char PAD_CHAR = '0';

	private static final String START_ROW_DATE = "D{DATE}D!";

	private static final String END_ROW_DATE = "D{DATE}D~";

	private static final String START_ROW_USER = "U{USERID}U!";

	private static final String END_ROW_USER = "U{USERID}U~";

	private static final String START_ROW_USER_DATE = "U{USERID}UD{DATE}D!";

	private static final String END_ROW_USER_DATE = "U{USERID}UD{DATE}D~";

	private static final String USER_ATTRVAL_SCORE_DETAIL_PATTERN = "I{DIMENSIONID}ID{DATE}DU{USERID}UA{ATTRVALID}A";

	private static final String START_ROW_USER_ATTRVAL_SCORE_DETAIL = "I{DIMENSIONID}ID{DATE}DU{USERID}U!";

	private static final String END_ROW_USER_ATTRVAL_SCORE_DETAIL = "I{DIMENSIONID}ID{DATE}DU{USERID}U~";
	
	private static final String START_ROW_USER_DATE_YYYYMMDD = "U{USERID}UD{DATE}!";
	
	private static final String END_ROW_USER_DATE_YYYYMMDD = "U{USERID}UD{DATE}~";
	
	private static final String START_ROW_DEVICE = "V{DEVICE}V!";

	private static final String END_ROW_DEVICE = "V{DEVICE}V~";

	public static String queryStartRowByDate(Date startDate) {
		String d = getDatePart(startDate);// 时间反转
		return StringUtils.replace(START_ROW_DATE, "{DATE}", d);
	}

	public static String queryEndRowByDate(Date date) {
		String d = getDatePart(date);// 时间反转
		return StringUtils.replace(END_ROW_DATE, "{DATE}", d);
	}
	
	public static String queryStartRowByDevice(String deviceId) {
		return StringUtils.replace(START_ROW_DEVICE, "{DEVICE}", deviceId);
	}

	public static String queryEndRowByDevice(String deviceId) {
		return StringUtils.replace(END_ROW_DEVICE, "{DEVICE}", deviceId);
	}
	
	public static String queryStartRowByUser(int startUserId) {
		String u = getUserIdPart(startUserId);
		return StringUtils.replace(START_ROW_USER, "{USERID}", u);
	}

	public static String queryEndRowByUser(int endUserId) {
		String u = getUserIdPart(endUserId);
		return StringUtils.replace(END_ROW_USER, "{USERID}", u);
	}

	private static String getDatePart(Date date) {
		return DateFormatUtils.format(date, "yyyyMMddHHmmss");
	}

	private static String getUserIdPart(int userId) {
		return StringUtils.leftPad(String.valueOf(userId), 10, PAD_CHAR);
	}

	public static String getUserAttrValScoreDetailRowKey(int dimensionId, Date date, int userId, String attrValId) {
		String dId = getDimensionIdPart(dimensionId);
		String d = getDatePart(date);
		String uId = getUserIdPart(userId);
		String avId = getAttrValIdPart(attrValId);
		return StringUtils.replaceEach(USER_ATTRVAL_SCORE_DETAIL_PATTERN, new String[] { "{DIMENSIONID}", "{DATE}", "{USERID}", "{ATTRVALID}" }, new String[] { dId, d, uId, avId });
	}

	private static String getDimensionIdPart(int dimensionId) {
		return StringUtils.leftPad(String.valueOf(dimensionId), 5, PAD_CHAR);// 最大支持维度数量：99999
	}

	private static String getAttrValIdPart(String attrValId) {
		return StringUtils.leftPad(attrValId, 5, PAD_CHAR);// 最大支持属性数量：99999
	}

	public static String getUserAttrValScoreDetailStartRowKey(int dimensionId, Date date, int userId) {
		String dId = getDimensionIdPart(dimensionId);
		String d = getDatePart(date);
		String uId = getUserIdPart(userId);
		return StringUtils.replaceEach(START_ROW_USER_ATTRVAL_SCORE_DETAIL, new String[] { "{DIMENSIONID}", "{DATE}", "{USERID}" }, new String[] { dId, d, uId });
	}

	public static String getUserAttrValScoreDetailEndRowKey(int dimensionId, Date date, int userId) {
		String dId = getDimensionIdPart(dimensionId);
		String d = getDatePart(date);
		String uId = getUserIdPart(userId);
		return StringUtils.replaceEach(END_ROW_USER_ATTRVAL_SCORE_DETAIL, new String[] { "{DIMENSIONID}", "{DATE}", "{USERID}" }, new String[] { dId, d, uId });
	}


	public static String getQueryStartRowKeyByUserAndStartDate(int userId, Date startDate) {
		String u = getUserIdPart(userId);
		String d = getDatePart(startDate);
		return StringUtils.replaceEach(START_ROW_USER_DATE, new String[] { "{USERID}", "{DATE}" }, new String[] { u, d });
	}

	public static String getQueryEndRowKeyByUserAndEndDate(int userId, Date endDate) {
		String u = getUserIdPart(userId);
		String d = getDatePart(endDate);
		return StringUtils.replaceEach(END_ROW_USER_DATE, new String[] { "{USERID}", "{DATE}" }, new String[] { u, d });
	}
	
	

}
