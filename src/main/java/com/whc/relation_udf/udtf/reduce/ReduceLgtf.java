package com.chinaoly.udtf.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class ReduceLgtf extends GenericUDTF{

	public void process(Object[] arg0)  throws HiveException{
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String lgdm = (String) arg0[1];
		String fjh = (String) arg0[2];
		try {
			JSONObject jsonObject = JSON.parseObject(jsondata);
			String lgmc = jsonObject.getString("lgmc");
			String xzqh = jsonObject.getString("xzqh");
			String lgdz = jsonObject.getString("lgdz");
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
						String rzsjA = personA.getString("rzsj");
						String ldsjA = personA.getString("ldsj");
						String rzsjB = personB.getString("rzsj");
						String ldsjB = personB.getString("ldsj");
						if (Tool.checkTzsTime(rzsjA, ldsjA, rzsjB, ldsjB, "yyyy-MM-dd HH:mm:ss", 900l)) {
							String xmA = personA.getString("xm");
							String xmB = personB.getString("xm");
							if (gmsfhmA.compareTo(gmsfhmB) > 0) {
								Object[] objects = {gmsfhmB, xmB, rzsjB, ldsjB, gmsfhmA, xmA, rzsjA, ldsjA, lgdm, lgmc, lgdz, fjh, xzqh};
		                        forward(objects);
//								forward(gmsfhmB, xmB, rzsjB, ldsjB, gmsfhmA, xmA, rzsjA, ldsjA, lgdm, lgmc, lgdz, fjh, xzqh);
							} else {
								Object[] objects = {gmsfhmA, xmA, rzsjA, ldsjA, gmsfhmB, xmB, rzsjB, ldsjB, lgdm, lgmc, lgdz, fjh, xzqh};
		                        forward(objects);
//								forward(gmsfhmA, xmA, rzsjA, ldsjA, gmsfhmB, xmB, rzsjB, ldsjB, lgdm, lgmc, lgdz, fjh, xzqh);
							}
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}
}
