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

public class ReduceZztf extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String dzbh = (String) arg0[1];
		String dzmc = (String) arg0[2];
		String xzqh = (String) arg0[3];
		String jd = (String) arg0[4];
		String wd = (String) arg0[5];
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
						String djsjA = personA.getString("djsj");
						String rylxA = personA.getString("rylx");
						String xmB = personB.getString("xm");
						String djsjB = personB.getString("djsj");
						String rylxB = personB.getString("rylx");
						if (gmsfhmA.compareTo(gmsfhmB) > 0) {
							forward(gmsfhmB, xmB, djsjB, rylxB, gmsfhmA, xmA, djsjA, rylxA, dzbh, dzmc, xzqh, jd, wd);
						} else {
							forward(gmsfhmA, xmA, djsjA, rylxA, gmsfhmB, xmB, djsjB, rylxB, dzbh, dzmc, xzqh, jd, wd);
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
