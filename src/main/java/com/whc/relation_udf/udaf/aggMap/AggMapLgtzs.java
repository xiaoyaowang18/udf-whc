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

//@Resolve({"string,string,string,string,string,string,string->string"})
@Resolve({"string,string,string,string,string,string->string"})
public class AggMapLgtzs extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String lgmc = "";
		private String xzqh = "";
		//private String lgdz = "";
		
		//private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			lgmc = in.readUTF();
			xzqh = in.readUTF();
			//lgdz = in.readUTF();
			//count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(lgmc);
			out.writeUTF(xzqh);
			//out.writeUTF(lgdz);
			//out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text lgmc = (Text)args[2];
		Text rzsj = (Text)args[3];
		Text ldsj = (Text)args[4];
		Text xzqh = (Text)args[5];
		//Text lgdz = (Text)args[6];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("rzsj", rzsj.toString());
		jsonObject.put("ldsj", ldsj.toString());
		
		JsonBuffer buf = (JsonBuffer) buffer;
		//if (buf.count <= 200) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.lgmc.length() == 0 && lgmc != null) {
				buf.lgmc = lgmc.toString();
			}
			if (buf.xzqh.length() == 0 && xzqh != null) {
				buf.xzqh = xzqh.toString();
			}
			/*if (buf.lgdz.length() == 0 && lgdz != null) {
				buf.lgdz = lgdz.toString();
			}*/
			//buf.count += 1;
		//}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		//if (buf.count + p.count <= 200) {
			buf.json = buf.json + p.json;
			if (buf.lgmc.length() == 0 && p.lgmc.length() > 0) {
				buf.lgmc = p.lgmc.toString();
			}
			if (buf.xzqh.length() == 0 && p.xzqh.length() > 0) {
				buf.xzqh = p.xzqh.toString();
			}
			/*if (buf.lgdz.length() == 0 && p.lgdz.length() > 0) {
				buf.lgdz = p.lgdz.toString();
			}*/
			//buf.count += p.count;
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
		//if (buf.count <= 200) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("lgmc", buf.lgmc);
			jsonObject.put("xzqh", buf.xzqh);
			//jsonObject.put("lgdz", buf.lgdz);
			jsondata = JSONObject.toJSONString(jsonObject);
		//}
		rel.set(jsondata);
		return rel;
	}

}
