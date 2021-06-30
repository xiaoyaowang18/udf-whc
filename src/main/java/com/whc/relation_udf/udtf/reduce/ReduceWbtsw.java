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

public class ReduceWbtsw extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String wbdm = (String) arg0[1];
		try {
			JSONObject jsonObject = JSON.parseObject(jsondata);
			String wbmc = jsonObject.getString("wbmc");
			String xzqh = jsonObject.getString("xzqh");
			//String wbdz = jsonObject.getString("wbdz");
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
						String sjsjA = personA.getString("sjsj");
						String xjsjA = personA.getString("xjsj");
						String sjsjB = personB.getString("sjsj");
						String xjsjB = personB.getString("xjsj");
						if (Tool.checkTzsTime(sjsjA, xjsjA, sjsjB, xjsjB, "yyyy-MM-dd HH:mm:ss", 300l)) {
							String xmA = personA.getString("xm");
							String xmB = personB.getString("xm");
							if (gmsfhmA.compareTo(gmsfhmB) > 0) {
								//forward(gmsfhmB, xmB, sjsjB, xjsjB, gmsfhmA, xmA, sjsjA, xjsjA, lgdm, wbmc, wbdz, xzqh);
								forward(gmsfhmB, xmB, sjsjB, xjsjB, gmsfhmA, xmA, sjsjA, xjsjA, wbdm, wbmc, xzqh);
								} else {
								//forward(gmsfhmA, xmA, sjsjA, xjsjA, gmsfhmB, xmB, sjsjB, xjsjB, lgdm, wbmc, wbdz, xzqh);
								forward(gmsfhmA, xmA, sjsjA, xjsjA, gmsfhmB, xmB, sjsjB, xjsjB, wbdm, wbmc, xzqh);
								}
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
