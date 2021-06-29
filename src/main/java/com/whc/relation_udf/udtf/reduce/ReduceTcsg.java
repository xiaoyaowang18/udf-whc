package com.chinaoly.udtf.reduce;

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

public class ReduceTcsg extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String sgbh = (String) arg0[1];
		try {
			JSONObject jsonObject = JSON.parseObject(jsondata);
			String hpzl = jsonObject.getString("hpzl");
			String hphm = jsonObject.getString("hphm");
			String jsrgmsfhm = jsonObject.getString("jsrgmsfhm");
			String jsrxm = jsonObject.getString("jsrxm");
			String[] jsonArr = jsonObject.getString("json").split("##");
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
						if (gmsfhmA.compareTo(gmsfhmB) > 0) {
							forward(gmsfhmB, xmB, gmsfhmA, xmA, jsrgmsfhm, jsrxm, hpzl, hphm, sgbh);
						} else {
							forward(gmsfhmA, xmA, gmsfhmB, xmB, jsrgmsfhm, jsrxm, hpzl, hphm, sgbh);
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
