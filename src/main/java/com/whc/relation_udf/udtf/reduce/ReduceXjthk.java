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

public class ReduceXjthk extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String xzqh = (String) arg0[1];
		String hh = (String) arg0[2];
		String hhid = "";
		try {
			JSONObject jsonObject = JSON.parseObject(jsondata);
			String zz = jsonObject.getString("zz");
			String hlx = jsonObject.getString("hlx");
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
						String yhzgxA = personA.getString("yhzgx");
						String djsjA = personA.getString("djsj");
						String xmA = personA.getString("xm");
						String yhzgxB = personB.getString("yhzgx");
						String djsjB = personB.getString("djsj");
						String xmB = personB.getString("xm");
						if (gmsfhmA.compareTo(gmsfhmB) > 0) {
							forward(gmsfhmB, xmB, yhzgxB, djsjB, gmsfhmA, xmA, yhzgxA, djsjA, hh, hlx, zz, xzqh, hhid);
						} else {
							forward(gmsfhmA, xmA, yhzgxA, djsjA, gmsfhmB, xmB, yhzgxB, djsjB, hh, hlx, zz, xzqh, hhid);
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
