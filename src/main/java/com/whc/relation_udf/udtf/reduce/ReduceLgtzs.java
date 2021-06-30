package com.whc.relation_udf.udtf.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class ReduceLgtzs extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub

		//上网时间差、下网时间差
		Long timeDifference = (Long) arg0[0];
		String jsondata = (String) arg0[1];
		String lgdm = (String) arg0[2];
		String lgmc = (String) arg0[3];
		String lgdz = (String) arg0[4];
		String xzqh = (String) arg0[5];
		try {
			String[] jsonArr = jsondata.split("##");
			Set<String> set = new HashSet<String>();
			//set存放每个人的上网记录去重
			for (String json : jsonArr) {
				if (json.length() > 0) {
					set.add(json);
				}
			}
			//存到list
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
						if (Tool.checkTzsTime(personAInfo[2], personAInfo[3], personBInfo[2], personBInfo[3], "yyyy-MM-dd HH:mm:ss", timeDifference)) {
							if (personAInfo[0].compareTo(personBInfo[0]) > 0) {
								forward(personBInfo[0], personBInfo[1], personBInfo[2], personBInfo[3], 
										personAInfo[0], personAInfo[1], personAInfo[2], personAInfo[3], 
										lgdm, lgmc, lgdz, xzqh);
							} else {
								forward(personAInfo[0], personAInfo[1], personAInfo[2], personAInfo[3], 
										personBInfo[0], personBInfo[1], personBInfo[2], personBInfo[3], 
										lgdm, lgmc, lgdz, xzqh);
							}
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
}
