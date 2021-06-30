package com.whc.relation_udf.udaf.aggMap;

import org.apache.commons.lang.StringUtils;

public class Tool {
	
	
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
	
}
