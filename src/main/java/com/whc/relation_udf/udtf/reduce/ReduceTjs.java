package com.chinaoly.udtf.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class ReduceTjs extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String jsbh = (String) arg0[1];
		String jsh = (String) arg0[2];
		String jsmc = (String) arg0[3];
		String jslb = (String) arg0[4];
		String jsdz = (String) arg0[5];
		try {
			String[] jsonArr = jsondata.split("##");
			Set<String> set = new HashSet<String>();
			for (String json : jsonArr) {
				if (json.length() > 0) {
					set.add(json);
				}
			}
			Iterator<String> it = set.iterator();
			List<String> list = new ArrayList<String>();
			while (it.hasNext()) {
				list.add(it.next());
			}
			if(list.size() > 1) {
				for (int i = 0; i < list.size(); i++) {
					String personA = list.get(i);
					for (int j = i + 1; j < list.size(); j++) {
						String personB = list.get(j);
						String[] personAInfo = personA.split("::");
						String[] personBInfo = personB.split("::");
						if (personAInfo[0].equals(personBInfo[0])) {
							continue;
						}
						if (Tool.checkTzzTime(personAInfo[2], personAInfo[3], personBInfo[2], personBInfo[3], "yyyy-MM-dd HH:mm:ss")) {
							if (personAInfo[0].compareTo(personBInfo[0]) > 0) {
								forward(personBInfo[0], personBInfo[1], personBInfo[2], personBInfo[3], 
										personAInfo[0], personAInfo[1], personAInfo[2], personAInfo[3], 
										jsbh, jsh, jsmc, jslb, jsdz);
							} else {
								forward(personAInfo[0], personAInfo[1], personAInfo[2], personAInfo[3], 
										personBInfo[0], personBInfo[1], personBInfo[2], personBInfo[3], 
										jsbh, jsh, jsmc, jslb, jsdz);
							}
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
