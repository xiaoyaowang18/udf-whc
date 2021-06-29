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

@Resolve({"string,string,string,string,string,string,string,string->string"})
public class AggMapTa extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String ajlb = "";
		private String czxq = "";
		private String jyaq = "";
		private String afdz = "";
		private String afsj = "";
		private String xzqh = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			ajlb = in.readUTF();
			czxq = in.readUTF();
			jyaq = in.readUTF();
			afdz = in.readUTF();
			afsj = in.readUTF();
			xzqh = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(ajlb);
			out.writeUTF(czxq);
			out.writeUTF(jyaq);
			out.writeUTF(afdz);
			out.writeUTF(afsj);
			out.writeUTF(xzqh);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text ajlb = (Text)args[2];
		Text czxq = (Text)args[3];
		Text jyaq = (Text)args[4];
		Text xzqh = (Text)args[5];
		Text afdz = (Text)args[6];
		Text afsj = (Text)args[7];
		
		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 50) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.ajlb.length() == 0 && ajlb != null) {
				buf.ajlb = ajlb.toString();
			}
			if (buf.czxq.length() == 0 && czxq != null) {
				buf.czxq = czxq.toString();
			}
			if (buf.jyaq.length() == 0 && jyaq != null) {
				buf.jyaq = jyaq.toString();
			}
			if (buf.xzqh.length() == 0 && xzqh != null) {
				buf.xzqh = xzqh.toString();
			}
			if (buf.afdz.length() == 0 && afdz != null) {
				buf.afdz = afdz.toString();
			}
			if (buf.afsj.length() == 0 && afsj != null) {
				buf.afsj = afsj.toString();
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
			if (buf.ajlb.length() == 0 && p.ajlb.length() > 0) {
				buf.ajlb = p.ajlb.toString();
			}
			if (buf.czxq.length() == 0 && p.czxq.length() > 0) {
				buf.czxq = p.czxq.toString();
			}
			if (buf.jyaq.length() == 0 && p.jyaq.length() > 0) {
				buf.jyaq = p.jyaq.toString();
			}
			if (buf.xzqh.length() == 0 && p.xzqh.length() > 0) {
				buf.xzqh = p.xzqh.toString();
			}
			if (buf.afdz.length() == 0 && p.afdz.length() > 0) {
				buf.afdz = p.afdz.toString();
			}
			if (buf.afsj.length() == 0 && p.afsj.length() > 0) {
				buf.afsj = p.afsj.toString();
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
			jsonObject.put("ajlb", buf.ajlb);
			jsonObject.put("czxq", buf.czxq);
			jsonObject.put("jyaq", buf.jyaq);
			jsonObject.put("xzqh", buf.xzqh);
			jsonObject.put("afdz", buf.afdz);
			jsonObject.put("afsj", buf.afsj);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
