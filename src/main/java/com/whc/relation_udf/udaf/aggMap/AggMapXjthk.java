package com.chinaoly.udaf.aggMap;

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
public class AggMapXjthk extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String zz = "";
		private String hlx = "";
//
		private int count = 0;

		
//		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			zz = in.readUTF();
			hlx = in.readUTF();
			count = in.readInt();
		}
//		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(zz);
			out.writeUTF(hlx);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text yhzgx = (Text)args[1];
		Text zz = (Text)args[2];
		Text hlx = (Text)args[3];
		Text djsj = (Text)args[4];
		Text xm = (Text)args[5];
		
		
		String yhzgxStr = null;
		if (yhzgx != null ) {
			yhzgxStr = yhzgx.toString();
		}
		String djsjStr = null;
		if (djsj != null ) {
			djsjStr = djsj.toString();
		}
		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("yhzgx", yhzgxStr);
		jsonObject.put("djsj", djsjStr);
		jsonObject.put("xm", xmStr);
		
		JsonBuffer buf = (JsonBuffer) buffer;
		
		if (buf.count <= 20) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.zz.length() == 0 && zz != null) {
				buf.zz = zz.toString();
			}
			if (buf.hlx.length() == 0 && hlx != null) {
				buf.hlx = hlx.toString();
			}
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
			if (buf.zz.length() == 0 && p.zz.length() > 0) {
				buf.zz = p.zz.toString();
			}
			if (buf.hlx.length() == 0 && p.hlx.length() > 0) {
				buf.hlx = p.hlx.toString();
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
		if (buf.count <= 20) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("zz", buf.zz);
			jsonObject.put("hlx", buf.hlx);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
