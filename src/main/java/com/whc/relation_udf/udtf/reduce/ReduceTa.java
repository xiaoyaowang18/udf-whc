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


public class ReduceTa extends GenericUDTF{

	@Override
	public void process(Object[] arg0) throws HiveException{
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String ajbh = (String) arg0[1];
		String ajly = (String) arg0[2];
		try {
			JSONObject jsonObject = JSON.parseObject(jsondata);
			String ajlb = jsonObject.getString("ajlb");
			String czxq = jsonObject.getString("czxq");
			String jyaq = jsonObject.getString("jyaq");
			String xzqh = jsonObject.getString("xzqh");
			String afdz = jsonObject.getString("afdz");
			String afsj = jsonObject.getString("afsj");
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
//							Object[] objects = {nodeB[0], nodeB[1], nodeB[2], nodeB[3], nodeA[0], nodeA[1], nodeA[2], nodeA[3], xthh, hlx, xxdz, ssxzqh};
							Object[] objects = {gmsfhmB, xmB, gmsfhmA, xmA, ajbh, null, ajlb, czxq, jyaq, afdz, afsj, xzqh, null, null, ajly};
							forward(objects);
		                    
//							forward(gmsfhmB, xmB, gmsfhmA, xmA, ajbh, null, ajlb, czxq, jyaq, afdz, afsj, xzqh, null, null, ajly);
						} else {
							Object[] objects = {gmsfhmA, xmA, gmsfhmB, xmB, ajbh, null, ajlb, czxq, jyaq, afdz, afsj, xzqh, null, null, ajly};
							forward(objects);
//							forward(gmsfhmA, xmA, gmsfhmB, xmB, ajbh, null, ajlb, czxq, jyaq, afdz, afsj, xzqh, null, null, ajly);
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
