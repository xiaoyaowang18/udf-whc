package com.whc.relation_udf.udaf.aggMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string,string,string->string"})
public class AggMR_hclph extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text ph = (Text)args[2];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("ph", ph.toString());
		
		JsonBuffer buf = (JsonBuffer) buffer;
		buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		buf.json = buf.json + p.json;
		
		
	}

	@Override
	public Writable newBuffer() {
		// TODO Auto-generated method stub
		return new JsonBuffer();
	}

	@Override
	public Writable terminate(Writable buffer) throws UDFException {
		JsonBuffer buf = (JsonBuffer) buffer;
		JSONArray result = new JSONArray();
		try {
			String[] jsonArr = buf.json.split("##");
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
						String phA = personA.getString("ph");
						String phB = personB.getString("ph");
						if (Tool.checkTrainTicket(phA, phB, 1)) {
							JSONObject jsonObject = new JSONObject();
							if (gmsfhmA.compareTo(gmsfhmB) > 0) {
								jsonObject.put("gmsfhm", gmsfhmB);
								jsonObject.put("xm", xmB);
								jsonObject.put("ph", phB);
								jsonObject.put("gxrgmsfhm", gmsfhmA);
								jsonObject.put("gxrxm", xmA);
								jsonObject.put("gxrph", phA);
							} else {
								jsonObject.put("gmsfhm", gmsfhmA);
								jsonObject.put("xm", xmA);
								jsonObject.put("ph", phA);
								jsonObject.put("gxrgmsfhm", gmsfhmB);
								jsonObject.put("gxrxm", xmB);
								jsonObject.put("gxrph", phB);
							}
							result.add(jsonObject);
						}
					}
				}
			}
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
		rel.set(JSON.toJSONString(result));
		return rel;
	}

}
