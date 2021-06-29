package com.chinaoly.udtf.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class ReduceColumns extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String type = (String) arg0[1];
		int objectlength = arg0.length;
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
					String personA = list.get(i) + " ";
					for (int j = i + 1; j < list.size(); j++) {
						String personB = list.get(j) + " ";
						String[] personAInfo = personA.split("::");
						String[] personBInfo = personB.split("::");
						int length = personAInfo.length;
						Object[] objectArr = new Object[objectlength + length + length - 1];
						if (personAInfo[0].equals(personBInfo[0])) {
							continue;
						}
						if (filter(type, personAInfo, personBInfo)) {
							if (personAInfo[0].compareTo(personBInfo[0]) > 0) {
								for (int l = 0; l < length; l++) {
									objectArr[l] = personBInfo[l].trim();
								}
								for (int l = 0; l < length; l++) {
									objectArr[l + length] = personAInfo[l].trim();
								}
							} else {
								for (int l = 0; l < length; l++) {
									objectArr[l] = personAInfo[l].trim();
								}
								for (int l = 0; l < length; l++) {
									objectArr[l + length] = personBInfo[l].trim();
								}
							}
							for (int l = 2; l < objectlength; l++) {
								objectArr[l + length + length - 2] = String.valueOf(arg0[l]);
							}
							forward(objectArr);
						}
						
					}
				}
			}
		} catch(Exception e) {
		}
	}
	private boolean filter (String type, String[] personAInfo, String[] personBInfo) {
		if ("fjthb".equals(type) || "hctcx".equals(type)) {
			return true;
		} else if ("hcttd".equals(type)) {
			return Tool.checkTjpTime(personAInfo[2], personBInfo[2], "yyyy-MM-dd HH:mm:ss", 30l);
		}
		return true;
	}
}
