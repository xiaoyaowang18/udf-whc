package com.chinaoly.udaf.aggMap;

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

@Resolve({"string,string,string,string,string,string->string"})
public class AggMR_ts extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text zzdjsj = (Text)args[2];
		Text zjdjsj = (Text)args[3];
		Text djcs = (Text)args[4];
		Text lybmc = (Text)args[5];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		String zzdjsjStr = null;
		if (zzdjsj != null ) {
			zzdjsjStr = zzdjsj.toString();
		}
		String zjdjsjStr = null;
		if (zjdjsj != null ) {
			zjdjsjStr = zjdjsj.toString();
		}
		String lybmcStr = null;
		if (lybmc != null ) {
			lybmcStr = lybmc.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("zzdjsj", zzdjsjStr);
		jsonObject.put("zjdjsj", zjdjsjStr);
		jsonObject.put("djcs", djcs.toString());
		jsonObject.put("lybmc", lybmcStr);
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 20) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			buf.count += 1;
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (buf.count + p.count <= 20) {
			buf.json = buf.json + p.json;
			buf.count += p.count;
		}
		
	}

	@Override
	public Writable newBuffer() {
		// TODO Auto-generated method stub
		return new JsonBuffer();
	}

	@Override
	public Writable terminate(Writable buffer) throws UDFException {
		JsonBuffer buf = (JsonBuffer) buffer;
		JSONArray jsonArray = new JSONArray();
		if (buf.count <= 20) {
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
							String zzdjsjA = personA.getString("zzdjsj");
							String zjdjsjA = personA.getString("zjdjsj");
							String zzdjsjB = personB.getString("zzdjsj");
							String zjdjsjB = personB.getString("zjdjsj");
							if (zzdjsjA.compareTo(zjdjsjB) <= 0 && zzdjsjB.compareTo(zjdjsjA) <= 0) {
								JSONObject jsonObject = new JSONObject();
								String xmA = personA.getString("xm");
								String djcsA = personA.getString("djcs");
								String lybmcA = personA.getString("lybmc");
								String xmB = personB.getString("xm");
								String djcsB = personB.getString("djcs");
								String lybmcB = personB.getString("lybmc");
								if (gmsfhmA.compareTo(gmsfhmB) > 0) {
									jsonObject.put("gmsfhm", gmsfhmB);
									jsonObject.put("xm", xmB);
									jsonObject.put("zzdjsj", zzdjsjB);
									jsonObject.put("zjdjsj", zjdjsjB);
									jsonObject.put("djcs", djcsB);
									jsonObject.put("ly", lybmcB);
									jsonObject.put("gxrgmsfhm", gmsfhmA);
									jsonObject.put("gxrxm", xmA);
									jsonObject.put("gxrzzdjsj", zzdjsjA);
									jsonObject.put("gxrzjdjsj", zjdjsjA);
									jsonObject.put("gxrdjcs", djcsA);
									jsonObject.put("gxrly", lybmcA);
								} else {
									jsonObject.put("gmsfhm", gmsfhmA);
									jsonObject.put("xm", xmA);
									jsonObject.put("zzdjsj", zzdjsjA);
									jsonObject.put("zjdjsj", zjdjsjA);
									jsonObject.put("djcs", djcsA);
									jsonObject.put("ly", lybmcA);
									jsonObject.put("gxrgmsfhm", gmsfhmB);
									jsonObject.put("gxrxm", xmB);
									jsonObject.put("gxrzzdjsj", zzdjsjB);
									jsonObject.put("gxrzjdjsj", zjdjsjB);
									jsonObject.put("gxrdjcs", djcsB);
									jsonObject.put("gxrly", lybmcB);
								}
								jsonArray.add(jsonObject);
							}
							
						}
					}
				}
			} catch(Exception e) {
				
			}
		}
		rel.set(JSON.toJSONString(jsonArray));
		return rel;
	}

}
