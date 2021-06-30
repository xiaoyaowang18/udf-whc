package com.whc.relation_udf.udtf.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class ReduceHclph extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String jsondata = (String) arg0[0];
		String cc = (String) arg0[1];
		String lcsj = (String) arg0[2];
		String lcsj_int = String.valueOf(arg0[3]);
		String cfz = (String) arg0[4];
		String ddz = (String) arg0[5];
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
						if (Tool.checkTrainTicket(personAInfo[2], personBInfo[2], 1)) {
							if (personAInfo[0].compareTo(personBInfo[0]) > 0) {
								forward(personBInfo[0], personBInfo[1], personBInfo[2], 
										personAInfo[0], personAInfo[1], personAInfo[2], 
										cc, lcsj, lcsj_int, cfz, ddz);
							} else {
								forward(personAInfo[0], personAInfo[1], personAInfo[2], 
										personBInfo[0], personBInfo[1], personBInfo[2], 
										cc, lcsj, lcsj_int, cfz, ddz);
							}
						}
					}
				}
			}
		} catch(Exception e) {
			
		}
	}
	
	
}
