package com.whc.relation_udf.udaf.aggMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string,string,string,string,string,string->string"})
public class AggMapTest extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private Set<String> set = new HashSet<String>();
		private String json = "";
		
		private String dgsj = "";
		private String sfd = "";
		private String mdd = ""; 

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			/*Iterator<String> it = set.iterator();
			StringBuffer jsonData = new StringBuffer();
			
			while (it.hasNext()) {
				jsonData.append("##" + it.next());
			}
			json = jsonData.toString();*/
			System.out.println(set);
			json = in.readUTF();
			dgsj = in.readUTF();
			sfd = in.readUTF();
			mdd = in.readUTF();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(dgsj);
			out.writeUTF(sfd);
			out.writeUTF(mdd);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text zwh = (Text)args[2];
		Text dgsj = (Text)args[3];
		Text sfd = (Text)args[4];
		Text mdd = (Text)args[5];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("zwh", zwh.toString());
		
		JsonBuffer buf = (JsonBuffer) buffer;
		buf.set.add(JSONObject.toJSONString(jsonObject));
		if (buf.dgsj.length() == 0 && dgsj != null) {
			buf.dgsj = dgsj.toString();
		}
		if (buf.sfd.length() == 0 && sfd != null) {
			buf.sfd = sfd.toString();
		}
		if (buf.mdd.length() == 0 && mdd != null) {
			buf.mdd = mdd.toString();
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		buf.set.addAll(p.set);
		if (buf.dgsj.length() == 0 && p.dgsj.length() > 0) {
			buf.dgsj = p.dgsj.toString();
		}
		if (buf.sfd.length() == 0 && p.sfd.length() > 0) {
			buf.sfd = p.sfd.toString();
		}
		if (buf.mdd.length() == 0 && p.mdd.length() > 0) {
			buf.mdd = p.mdd.toString();
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
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("json", buf.json);
		jsonObject.put("dgsj", buf.dgsj);
		jsonObject.put("sfd", buf.sfd);
		jsonObject.put("mdd", buf.mdd);
		rel.set(JSONObject.toJSONString(jsonObject));
		return rel;
	}

}
