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
public class AggMapTzz extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String fwdz = "";
		private String fzgmsfhm = "";
		private String fzxm = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			fwdz = in.readUTF();
			fzgmsfhm = in.readUTF();
			fzxm = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(fwdz);
			out.writeUTF(fzgmsfhm);
			out.writeUTF(fzxm);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text fwdz = (Text)args[2];
		Text fzgmsfhm = (Text)args[3];
		Text fzxm = (Text)args[4];
		Text djrq = (Text)args[5];
		Text dqrq = (Text)args[6];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("djrq", djrq.toString());
		jsonObject.put("dqrq", dqrq.toString());
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 50) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.fwdz.length() == 0 && fwdz != null) {
				buf.fwdz = fwdz.toString();
			}
			if (buf.fzgmsfhm.length() == 0 && fzgmsfhm != null) {
				buf.fzgmsfhm = fzgmsfhm.toString();
			}
			if (buf.fzxm.length() == 0 && fzxm != null) {
				buf.fzxm = fzxm.toString();
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
			if (buf.fwdz.length() == 0 && p.fwdz.length() > 0) {
				buf.fwdz = p.fwdz.toString();
			}
			if (buf.fzgmsfhm.length() == 0 && p.fzgmsfhm.length() > 0) {
				buf.fzgmsfhm = p.fzgmsfhm.toString();
			}
			if (buf.fzxm.length() == 0 && p.fzxm.length() > 0) {
				buf.fzxm = p.fzxm.toString();
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
			jsonObject.put("fwdz", buf.fwdz);
			jsonObject.put("fzgmsfhm", buf.fzgmsfhm);
			jsonObject.put("fzxm", buf.fzxm);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
