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

@Resolve({"string,string,string,string,string,string,string->string"})
public class AggMapTjs extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String jsmc = "";
		private String jslb = "";
		private String jsdz = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			jsmc = in.readUTF();
			jslb = in.readUTF();
			jsdz = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(jsmc);
			out.writeUTF(jslb);
			out.writeUTF(jsdz);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text rssj = (Text)args[2];
		Text lssj = (Text)args[3];
		Text jsmc = (Text)args[4];
		Text jslb = (Text)args[5];
		Text jsdz = (Text)args[6];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("rssj", rssj.toString());
		jsonObject.put("lssj", lssj.toString());
		
		JsonBuffer buf = (JsonBuffer) buffer;
		//if (buf.count <= 500) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.jsmc.length() == 0 && jsmc != null) {
				buf.jsmc = jsmc.toString();
			}
			if (buf.jslb.length() == 0 && jslb != null) {
				buf.jslb = jslb.toString();
			}
			if (buf.jsdz.length() == 0 && jsdz != null) {
				buf.jsdz = jsdz.toString();
			}
			buf.count += 1;
		//}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		//if (buf.count + p.count <= 500) {
			buf.json = buf.json + p.json;
			if (buf.jsmc.length() == 0 && p.jsmc.length() > 0) {
				buf.jsmc = p.jsmc.toString();
			}
			if (buf.jslb.length() == 0 && p.jslb.length() > 0) {
				buf.jslb = p.jslb.toString();
			}
			if (buf.jsdz.length() == 0 && p.jsdz.length() > 0) {
				buf.jsdz = p.jsdz.toString();
			}
			buf.count += p.count;
		//} 
		
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
		//if (buf.count <= 500) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("jsmc", buf.jsmc);
			jsonObject.put("jslb", buf.jslb);
			jsonObject.put("jsdz", buf.jsdz);
			jsondata = JSONObject.toJSONString(jsonObject);
		//}
		rel.set(jsondata);
		return rel;
	}

}
