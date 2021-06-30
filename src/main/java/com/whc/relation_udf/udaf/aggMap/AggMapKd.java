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

@Resolve({"string,string,string,string,string,string->string"})
public class AggMapKd extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String hbh = "";
		private String hdz = "";
		private String xzddz = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			hbh = in.readUTF();
			hdz = in.readUTF();
			xzddz = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(hbh);
			out.writeUTF(hdz);
			out.writeUTF(xzddz);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text yhzgx = (Text)args[2];
		Text hbh = (Text)args[3];
		Text hdz = (Text)args[4];
		Text xzddz = (Text)args[5];
		
		
		String yhzgxStr = null;
		if (yhzgx != null ) {
			yhzgxStr = yhzgx.toString();
		}
		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("yhzgx", yhzgxStr);
		jsonObject.put("xm", xmStr);
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 50) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.hbh.length() == 0 && hbh != null) {
				buf.hbh = hbh.toString();
			}
			if (buf.hdz.length() == 0 && hdz != null) {
				buf.hdz = hdz.toString();
			}
			if (buf.xzddz.length() == 0 && xzddz != null) {
				buf.xzddz = xzddz.toString();
			}
			buf.count += 1;
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (buf.count + p.count <= 50) {
			buf.json = buf.json + p.json;
			if (buf.hbh.length() == 0 && p.hbh.length() > 0) {
				buf.hbh = p.hbh.toString();
			}
			if (buf.hdz.length() == 0 && p.hdz.length() > 0) {
				buf.hdz = p.hdz.toString();
			}
			if (buf.xzddz.length() == 0 && p.xzddz.length() > 0) {
				buf.xzddz = p.xzddz.toString();
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
		if (buf.count <= 50) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("hbh", buf.hbh);
			jsonObject.put("hdz", buf.hdz);
			jsonObject.put("xzddz", buf.xzddz);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
