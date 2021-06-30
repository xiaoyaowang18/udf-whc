package com.whc.relation_udf.udaf.aggMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string,string,string,string,string->string"})
public class AggMapTcsg extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String hpzl = "";
		private String hphm = "";
		private String jsrgmsfhm = "";
		private String jsrxm = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			hpzl = in.readUTF();
			hphm = in.readUTF();
			jsrgmsfhm = in.readUTF();
			jsrxm = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(hpzl);
			out.writeUTF(hphm);
			out.writeUTF(jsrgmsfhm);
			out.writeUTF(jsrxm);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text hpzl = (Text)args[2];
		Text hphm = (Text)args[3];
		Text czclbh = (Text)args[4];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 10) {
			if (czclbh == null) {
				if (buf.hpzl.length() == 0 && hpzl != null) {
					buf.hpzl = hpzl.toString();
				}
				if (buf.hphm.length() == 0 && hphm != null) {
					buf.hphm = hphm.toString();
				}
				if (buf.jsrgmsfhm.length() == 0) {
					buf.jsrgmsfhm = gmsfhm.toString();
				}
				if (buf.jsrxm.length() == 0 && xmStr != null) {
					buf.jsrxm = xmStr;
				}
			} else {
				buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			}
			buf.count += 1;
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (buf.count + p.count <= 10) {
			buf.json = buf.json + p.json;
			if (buf.hpzl.length() == 0 && p.hpzl.length() > 0) {
				buf.hpzl = p.hpzl.toString();
			}
			if (buf.hphm.length() == 0 && p.hphm.length() > 0) {
				buf.hphm = p.hphm.toString();
			}
			if (buf.jsrgmsfhm.length() == 0 && p.jsrgmsfhm.length() > 0) {
				buf.jsrgmsfhm = p.jsrgmsfhm.toString();
			}
			if (buf.jsrxm.length() == 0 && p.jsrxm.length() > 0) {
				buf.jsrxm = p.jsrxm.toString();
			}
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
		String jsondata = "";
		if (buf.count > 1 && buf.count <= 10) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("hpzl", buf.hpzl);
			jsonObject.put("hphm", buf.hphm);
			jsonObject.put("jsrgmsfhm", buf.jsrgmsfhm);
			jsonObject.put("jsrxm", buf.jsrxm);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
