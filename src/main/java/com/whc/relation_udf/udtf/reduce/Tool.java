package com.chinaoly.udtf.reduce;

import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;



public class Tool {
	
	private final static String[] strDigits = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};
	
	public static boolean checkTzzTime(String djrq1, String dqrq1, String djrq2, String dqrq2, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Date startDate1 = null;
		Date endDate1 = null;
		Date startDate2 = null;
		Date endDate2 = null;
		boolean result = false;
		try {
			startDate1 = sdf.parse(djrq1);
			startDate2 = sdf.parse(djrq2);
			endDate1 = sdf.parse(dqrq1);
			endDate2 = sdf.parse(dqrq2);
			if (startDate1.getTime() < endDate2.getTime() && endDate1.getTime() > startDate2.getTime()) {
		    	if (startDate1.getTime() != endDate1.getTime() && startDate2.getTime() != endDate2.getTime()) {
		        	result = true;
		        }
		    }
		} catch (Exception e) {
			result = false;
		}
		return result;
	}
	
	public static boolean checkTzsTime(String djrq1, String dqrq1, String djrq2, String dqrq2, String pattern, long seconds) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Date startDate1 = null;
		Date endDate1 = null;
		Date startDate2 = null;
		Date endDate2 = null;
		boolean result = false;
		long milliseconds = seconds * 1000;
		try {

			startDate1 = sdf.parse(djrq1);
			startDate2 = sdf.parse(djrq2);
			endDate1 = sdf.parse(dqrq1);
			endDate2 = sdf.parse(dqrq2);
			long startTimeDifference = startDate1.getTime() - startDate2.getTime();
			long endTimeDifference = endDate1.getTime() - endDate2.getTime();
			if (startTimeDifference < milliseconds && startTimeDifference > milliseconds * -1 && endTimeDifference < milliseconds && endTimeDifference > milliseconds * -1) {
				result = true;
			}
		} catch (Exception e) {
			result = false;
		}
		return result;
	}
	
	public static boolean checkTjpTime(String djrq1, String djrq2, String pattern, long seconds) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Date startDate1 = null;
		Date startDate2 = null;
		boolean result = false;
		long milliseconds = seconds * 1000;
		try {

			startDate1 = sdf.parse(djrq1);
			startDate2 = sdf.parse(djrq2);
			long startTimeDifference = startDate1.getTime() - startDate2.getTime();
			if (startTimeDifference < milliseconds && startTimeDifference > milliseconds * -1) {
				result = true;
			}
		} catch (Exception e) {
			result = false;
		}
		return result;
	}
	
	//返回形式为数字跟字符串
	private static String byteToArrayString(byte bByte) {
		int iRet = bByte;
		if (iRet < 0) {
			iRet += 256;
		}
		return strDigits[iRet / 16] + strDigits[iRet % 16];
	}
	//转换字节数组为16进制字串
	private static String byteToString(byte[] bByte) {
		StringBuffer sBuffer = new StringBuffer();
		for (int i = 0; i < bByte.length; i++) {
			sBuffer.append(byteToArrayString(bByte[i]));
		}
		return sBuffer.toString();
	}
	
	public static String GetMD5Code(String strObj) {
		String resultString = "";
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			resultString = byteToString(md.digest(strObj.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultString;
	}
	
	public static boolean checkTrainTicket(String ticket1, String ticket2, int length) {
		if (ticket1 != null && ticket2 != null && ticket1.length() == 7 && ticket2.length() == 7 && (!ticket1.equals(ticket2))) {

			int x1 = Integer.parseInt(StringUtils.substring(ticket1, 1, 7));
			int x2 = Integer.parseInt(StringUtils.substring(ticket2, 1, 7));
			char c1 = ticket1.charAt(0);
			char c2 = ticket2.charAt(0);

			if (c1 == c2 && Math.abs(x1 - x2) <= length) {
				return true;
			}
		}
		
		return false;
	}
	
	public static boolean checkFlightSeat(String seat1, String seat2, String seat_all, int length) {
		if (seat1 != null && seat2 != null && (!seat1.equals(seat2))) {
			int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, seat1.length() - 1));
			int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, seat2.length() - 1));
			int seatSize = seat_all.length();
			if ((Math.abs(x1 - x2) - 1) * seatSize + 1 <= length) {
				int y1 = StringUtils.indexOf(seat_all, seat1.charAt(seat1.length() - 1)) + 1;
				int y2 = StringUtils.indexOf(seat_all, seat2.charAt(seat2.length() - 1)) + 1;
				if (y1 != 0 && y2 != 0) {
					if (Math.abs((x1 * seatSize + y1) - (x2 * seatSize + y2)) <= length) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	public static boolean checkTrainSeat(String seat1, String seat2, String type, int length) {
		if (seat1 != null && seat2 != null && seat1.trim().length() == 4 && seat2.trim().length() == 4 && (!seat1.equals(seat2))) {
			//二等座
			if ("O".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				char c1 = seat1.charAt(3);
				char c2 = seat2.charAt(3);
				int y1 = -1;
				switch (c1) {
					case 'A': y1 = 1; break;
					case 'B': y1 = 2; break;
					case 'C': y1 = 3; break;
					case 'D': y1 = 4; break;
					case 'F': y1 = 5; break;
				}
				int y2 = -1;
				switch (c2) {
					case 'A': y2 = 1; break;
					case 'B': y2 = 2; break;
					case 'C': y2 = 3; break;
					case 'D': y2 = 4; break;
					case 'F': y2 = 5; break;
				}
				if (y1 != -1 && y2 != -1) {
					//Math.abs((3 * 10 + 5) - (5 * 10 + 4) - (3 - 5) * (10 - 5)) <= 2
					//	Math.abs((35 - 54 - (-2) * 5) = 1 <=2 
					//		false
					if (Math.abs((x1 * 5 + y1) - (x2 * 5 + y2)) <= length) {
						return true;
					}
				}
			}
			//一等座
			if ("M".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				char c1 = seat1.charAt(3);
				char c2 = seat2.charAt(3);
				int y1 = -1;
				switch (c1) {
					case 'A': y1 = 1; break;
					case 'C': y1 = 2; break;
					case 'D': y1 = 3; break;
					case 'F': y1 = 4; break;
				}
				int y2 = -1;
				switch (c2) {
					case 'A': y2 = 1; break;
					case 'C': y2 = 2; break;
					case 'D': y2 = 3; break;
					case 'F': y2 = 4; break;
				}
				if (y1 != -1 && y2 != -1) {
					//Math.abs((1 * 10 + 4) - (2 * 10 + 1) - (1 - 2) * (10 - 4)) <= 2
					//	Math.abs(14 - 21 - (-1) * 6) = 1 <=2 
					//		false
					if (Math.abs((x1 * 4 + y1) - (x2 * 4 + y2)) <= length) {
						return true;
					}
				}
			}
			
			if ("1".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				char c1 = seat1.charAt(3);
				char c2 = seat2.charAt(3);
				int y1 = -1;
				switch (c1) {
					case 'A': y1 = 1; break;
					case 'B': y1 = 2; break;
					case 'C': y1 = 3; break;
					case 'D': y1 = 4; break;
					case 'a': y1 = 1; break;
					case 'b': y1 = 2; break;
					case 'c': y1 = 3; break;
					case 'd': y1 = 4; break;
				}
				int y2 = -1;
				switch (c2) {
					case 'A': y2 = 1; break;
					case 'B': y2 = 2; break;
					case 'C': y2 = 3; break;
					case 'D': y2 = 4; break;
					case 'a': y2 = 1; break;
					case 'b': y2 = 2; break;
					case 'c': y2 = 3; break;
					case 'd': y2 = 4; break;
				}
				if (y1 != -1 && y2 != -1) {
					//Math.abs((1 * 10 + 4 - 2 * 10 + 1) - (1 - 2) * (10 - 4)) <= 2
					//	Math.abs((14 - 21) - (-1) * 6) = 1 <=2 
					//		false
					if (Math.abs((x1 * 4 + y1) - (x2 * 4 + y2)) <= length) {
						return true;
					}
				} else if (y1 == -1 && y2 == -1) {
					if (Math.abs(Integer.parseInt(seat1) - Integer.parseInt(seat2)) <= length) {
						return true;
					}
				}
			}
			
			if ("2".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				char c1 = seat1.charAt(3);
				char c2 = seat2.charAt(3);
				int y1 = -1;
				switch (c1) {
					case 'A': y1 = 1; break;
					case 'B': y1 = 2; break;
					case 'C': y1 = 3; break;
					case 'a': y1 = 1; break;
					case 'b': y1 = 2; break;
					case 'c': y1 = 3; break;
				}
				int y2 = -1;
				switch (c2) {
					case 'A': y2 = 1; break;
					case 'B': y2 = 2; break;
					case 'C': y2 = 3; break;
					case 'a': y2 = 1; break;
					case 'b': y2 = 2; break;
					case 'c': y2 = 3; break;
				}
				if (y1 != -1 && y2 != -1) {
					if (Math.abs((x1 * 3 + y1) - (x2 * 3 + y2)) <= length) {
						return true;
					}
				} else if (y1 == -1 && y2 == -1) {
					if (Math.abs(Integer.parseInt(seat1) - Integer.parseInt(seat2)) <= length) {
						return true;
					}
				}
			}
			
			//特等座
			if ("9".equals(type) || "P".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				char c1 = seat1.charAt(3);
				char c2 = seat2.charAt(3);
				int y1 = -1;
				switch (c1) {
					case 'A': y1 = 1; break;
					case 'C': y1 = 2; break;
					case 'F': y1 = 3; break;
				}
				int y2 = -1;
				switch (c2) {
					case 'A': y2 = 1; break;
					case 'C': y2 = 2; break;
					case 'F': y2 = 3; break;
				}
				if (y1 != -1 && y2 != -1) {
					//Math.abs((1 * 10 + 3) - (2 * 10 + 1) - (1 - 2) * (10 - 3)) <= 2
					//	Math.abs(13 - 21 - (-1) * 7) = 1 <=2 
					//		false
					if (Math.abs((x1 * 3 + y1) - (x2 * 3 + y2)) <= length) {
						return true;
					}
				}
			}
			
			if ("3".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				int y1 = -1;
				int y2 = -1;
				y1 = Integer.parseInt(StringUtils.substring(seat1, 3, 4));
				y2 = Integer.parseInt(StringUtils.substring(seat2, 3, 4));
				if (y1 != -1 && y2 != -1) {
					//Math.abs((1 * 10 + 3 - 2 * 10 + 1) - (1 - 2) * (10 - 3)) <= 2
					//	Math.abs((13 - 21) - (-1) * 7) = 1 <=2 
					//		false
					if (Math.abs((x1 * 3 + y1) - (x2 * 3 + y2)) <= length) {
						return true;
					}
				}
			}
			
			//结尾只有1和3
			if ("F".equals(type) || "A".equals(type) || "6".equals(type) || "4".equals(type)) {
				int x1 = Integer.parseInt(StringUtils.substring(seat1, 0, 3));
				int x2 = Integer.parseInt(StringUtils.substring(seat2, 0, 3));
				char c1 = seat1.charAt(3);
				char c2 = seat2.charAt(3);
				int y1 = -1;
				switch (c1) {
					case '1': y1 = 1; break;
					case '3': y1 = 2; break;
				}
				int y2 = -1;
				switch (c2) {
					case '1': y2 = 1; break;
					case '3': y2 = 2; break;
				}
				if (y1 != -1 && y2 != -1) {
					//Math.abs((1 * 10 + 3) - (2 * 10 + 1) - (1 - 2) * (10 - 3)) <= 2
					//	Math.abs(13 - 21 - (-1) * 7) = 1 <=2 
					//		false
					if (Math.abs((x1 * 2 + y1) - (x2 * 2 + y2)) <= length) {
						return true;
					}
				}
			}
			
			//无座
			if ("W".equals(type) || "8".equals(type) || "7".equals(type)) {
				if (Math.abs(Integer.parseInt(seat1) - Integer.parseInt(seat2)) <= length) {
					return true;
				}
			}
			
		}
		
		return false;
	}
}
