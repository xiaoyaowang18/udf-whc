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

public class ReduceTs extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String wpdm = (String) arg0[1];
		String wplb = (String) arg0[2];
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
						String zzdjsjA = personA.getString("zzdjsj");
						String zjdjsjA = personA.getString("zjdjsj");
						String zzdjsjB = personB.getString("zzdjsj");
						String zjdjsjB = personB.getString("zjdjsj");
						if (zzdjsjA.compareTo(zjdjsjB) <= 0 && zzdjsjB.compareTo(zjdjsjA) <= 0) {
							String xmA = personA.getString("xm");
							String djcsA = personA.getString("djcs");
							String lybmcA = personA.getString("lybmc");
							String xmB = personB.getString("xm");
							String djcsB = personB.getString("djcs");
							String lybmcB = personB.getString("lybmc");
							if (gmsfhmA.compareTo(gmsfhmB) > 0) {
								forward(gmsfhmB, xmB, zzdjsjB, zjdjsjB, djcsB, lybmcB, gmsfhmA, xmA, zzdjsjA, zjdjsjA, djcsA, lybmcA, wpdm, null, wplb);
							} else {
								forward(gmsfhmA, xmA, zzdjsjA, zjdjsjA, djcsA, lybmcA, gmsfhmB, xmB, zzdjsjB, zjdjsjB, djcsB, lybmcB, wpdm, null, wplb);
							}
						}
						
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
