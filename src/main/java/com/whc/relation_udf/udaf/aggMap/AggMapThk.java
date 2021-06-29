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
public class AggMapThk extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String zz = "";
		private String hlx = "";
		private String xzqhmc = "";
		private String hh = "";
		
		private int count = 0;
		//同一户号下所有数据的yhzgx与户主关系值不全为非亲属或99
		private boolean flag_99 = false;
		//同一户号下所有数据中存在一条yhzgx与户主关系值为小集体户户主或03
		private boolean flag_03 = true;

		
//		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			zz = in.readUTF();
			hlx = in.readUTF();
			xzqhmc = in.readUTF();
			hh = in.readUTF();
			count = in.readInt();
			flag_99 = in.readBoolean();
			flag_03 = in.readBoolean();
		}
//		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(zz);
			out.writeUTF(hlx);
			out.writeUTF(xzqhmc);
			out.writeUTF(hh);
			out.writeInt(count);
			out.writeBoolean(flag_99);
			out.writeBoolean(flag_03);
		}
	}

	private Text rel = new Text();
	
//	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text yhzgx = (Text)args[1];
		Text zz = (Text)args[2];
		Text hlx = (Text)args[3];
		Text djsj = (Text)args[4];
		Text xm = (Text)args[5];
		Text xzqhmc = (Text)args[6];
		Text hh = (Text)args[7];
		
		
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
		
		if ("小集体户户主".equals(yhzgxStr) || "03".equals(yhzgxStr)) {
			buf.flag_03 = false;
		}
		if (!("非亲属".equals(yhzgxStr) || "99".equals(yhzgxStr))) {
			buf.flag_99 = true;
		}
		if (buf.flag_03 && buf.count <= 10) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.zz.length() == 0 && zz != null) {
				buf.zz = zz.toString();
			}
			if (buf.hlx.length() == 0 && hlx != null) {
				buf.hlx = hlx.toString();
			}
			if (buf.xzqhmc.length() == 0 && xzqhmc != null) {
				buf.xzqhmc = xzqhmc.toString();
			}
			if (buf.hh.length() == 0 && hh != null) {
				buf.hh = hh.toString();
			}
			buf.count += 1;
		}
	}

//	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (buf.flag_03 && !p.flag_03) {
			buf.flag_03 = false;
		}
		if (p.flag_99) {
			buf.flag_99 = true;
		}
		if (buf.flag_03 && buf.count + p.count <= 10) {
			buf.json = buf.json + p.json;
			if (buf.zz.length() == 0 && p.zz.length() > 0) {
				buf.zz = p.zz.toString();
			}
			if (buf.hlx.length() == 0 && p.hlx.length() > 0) {
				buf.hlx = p.hlx.toString();
			}
			if (buf.xzqhmc.length() == 0 && p.xzqhmc.length() > 0) {
				buf.xzqhmc = p.xzqhmc.toString();
			}
			if (buf.hh.length() == 0 && p.hh.length() > 0) {
				buf.hh = p.hh.toString();
			}
			buf.count += p.count;
		}
		
	}

//	@Override
	public Writable newBuffer() {
		// TODO Auto-generated method stub
		return new JsonBuffer();
	}

//	@Override
	public Writable terminate(Writable buffer) throws UDFException {
		JsonBuffer buf = (JsonBuffer) buffer;
		String jsondata = "";
		if (buf.flag_03 && buf.flag_99 && buf.count <= 10) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("zz", buf.zz);
			jsonObject.put("hlx", buf.hlx);
			jsonObject.put("xzqhmc", buf.xzqhmc);
			jsonObject.put("hh", buf.hh);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
