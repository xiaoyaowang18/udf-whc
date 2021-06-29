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

@Resolve({"string,string,string,string,string,string,string,string,string->string"})
public class AggMapTxd extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String bxdrxm = "";
		private String fwdah = "";
		private String fwdz = "";
		private String djsj = "";
		private String dqsj = "";
		private String fzgmsfhm = "";
		private String fzxm = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			bxdrxm = in.readUTF();
			fwdah = in.readUTF();
			fwdz = in.readUTF();
			djsj = in.readUTF();
			dqsj = in.readUTF();
			fzgmsfhm = in.readUTF();
			fzxm = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(bxdrxm);
			out.writeUTF(fwdah);
			out.writeUTF(fwdz);
			out.writeUTF(djsj);
			out.writeUTF(dqsj);
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
		Text bxdrxm = (Text)args[2];
		Text fwdah = (Text)args[3];
		Text fwdz = (Text)args[4];
		Text djsj = (Text)args[5];
		Text dqsj = (Text)args[6];
		Text fzgmsfhm = (Text)args[7];
		Text fzxm = (Text)args[8];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 20) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.bxdrxm.length() == 0 && bxdrxm != null) {
				buf.bxdrxm = bxdrxm.toString();
			}
			if (buf.fwdah.length() == 0 && fwdah != null) {
				buf.fwdah = fwdah.toString();
			}
			if (buf.fwdz.length() == 0 && fwdz != null) {
				buf.fwdz = fwdz.toString();
			}
			if (buf.djsj.length() == 0 && djsj != null) {
				buf.djsj = djsj.toString();
			}
			if (buf.dqsj.length() == 0 && dqsj != null) {
				buf.dqsj = dqsj.toString();
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
			if (buf.bxdrxm.length() == 0 && p.bxdrxm.length() > 0) {
				buf.bxdrxm = p.bxdrxm.toString();
			}
			if (buf.fwdah.length() == 0 && p.fwdah.length() > 0) {
				buf.fwdah = p.fwdah.toString();
			}
			if (buf.fwdz.length() == 0 && p.fwdz.length() > 0) {
				buf.fwdz = p.fwdz.toString();
			}
			if (buf.djsj.length() == 0 && p.djsj.length() > 0) {
				buf.djsj = p.djsj.toString();
			}
			if (buf.dqsj.length() == 0 && p.dqsj.length() > 0) {
				buf.dqsj = p.dqsj.toString();
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
			jsonObject.put("bxdrxm", buf.bxdrxm);
			jsonObject.put("fwdah", buf.fwdah);
			jsonObject.put("fwdz", buf.fwdz);
			jsonObject.put("djsj", buf.djsj);
			jsonObject.put("dqsj", buf.dqsj);
			jsonObject.put("fzgmsfhm", buf.fzgmsfhm);
			jsonObject.put("fzxm", buf.fzxm);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
