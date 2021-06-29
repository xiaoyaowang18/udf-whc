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
public class AggMapWbtsw extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String wbmc = "";
		private String xzqh = "";
		//private String wbdz = "";
		
		//private int count = 0;

		
//		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			wbmc = in.readUTF();
			xzqh = in.readUTF();
			//wbdz = in.readUTF();
			//count = in.readInt();
		}
//		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(wbmc);
			out.writeUTF(xzqh);
			//out.writeUTF(wbdz);
			//out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text wbmc = (Text)args[2];
		Text sjsj = (Text)args[3];
		Text xjsj = (Text)args[4];
		Text xzqh = (Text)args[5];
		//Text wbdz = (Text)args[6];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("sjsj", sjsj.toString());
		jsonObject.put("xjsj", xjsj.toString());
		
		JsonBuffer buf = (JsonBuffer) buffer;
		//if (buf.count <= 200) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.wbmc.length() == 0 && wbmc != null) {
				buf.wbmc = wbmc.toString();
			}
			if (buf.xzqh.length() == 0 && xzqh != null) {
				buf.xzqh = xzqh.toString();
			}
			/*if (buf.wbdz.length() == 0 && wbdz != null) {
				buf.wbdz = wbdz.toString();
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
			if (buf.wbmc.length() == 0 && p.wbmc.length() > 0) {
				buf.wbmc = p.wbmc.toString();
			}
			if (buf.xzqh.length() == 0 && p.xzqh.length() > 0) {
				buf.xzqh = p.xzqh.toString();
			}
			/*if (buf.wbdz.length() == 0 && p.wbdz.length() > 0) {
				buf.wbdz = p.wbdz.toString();
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
			jsonObject.put("wbmc", buf.wbmc);
			jsonObject.put("xzqh", buf.xzqh);
			//jsonObject.put("wbdz", buf.wbdz);
			jsondata = JSONObject.toJSONString(jsonObject);
		//}
		rel.set(jsondata);
		return rel;
	}

}
