package com.whc.relation_udf.udtf.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class ReduceFjlz extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String hbh = (String) arg0[1];
		String lgsj = (String) arg0[2];
		String lgsj_int = String.valueOf(arg0[3]);
		String dgsj = (String) arg0[4];
		String dgsj_int = String.valueOf(arg0[5]);
		String sfd = (String) arg0[6];
		String mdd = (String) arg0[7];
		String cwdj = (String) arg0[8];
		String zwh_all = (String) arg0[9];
		try {
			String[] jsonArr = jsondata.split("##");
			Set<String> set = new HashSet<String>();
			for (String json : jsonArr) {
				if (json.length() > 0) {
					set.add(json);
				}
			}
			Iterator<String> it = set.iterator();
			List<JSONObject> list = new ArrayList<JSONObject>();
			while (it.hasNext()) {
				list.add(JSON.parseObject(it.next()));
			}
			if(list.size() > 1) {
				for (int i = 0; i < list.size(); i++) {
					JSONObject personA = list.get(i);
					for (int j = i + 1; j < list.size(); j++) {
						JSONObject personB = list.get(j);
						String gmsfhmA = personA.getString("gmsfhm");
						String gmsfhmB = personB.getString("gmsfhm");
						if (gmsfhmA.equals(gmsfhmB)) {
							continue;
						}
						String xmA = personA.getString("xm");
						String xmB = personB.getString("xm");
						String zwhA = personA.getString("zwh");
						String zwhB = personB.getString("zwh");
						if (Tool.checkFlightSeat(zwhA, zwhB, zwh_all, 1)) {
							if (gmsfhmA.compareTo(gmsfhmB) > 0) {
								forward(gmsfhmB, xmB, zwhB, gmsfhmA, xmA, zwhA, hbh, lgsj, lgsj_int, dgsj, dgsj_int, sfd, mdd, cwdj);
							} else {
								forward(gmsfhmA, xmA, zwhA, gmsfhmB, xmB, zwhB, hbh, lgsj, lgsj_int, dgsj, dgsj_int, sfd, mdd, cwdj);
							}
						}
						
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
