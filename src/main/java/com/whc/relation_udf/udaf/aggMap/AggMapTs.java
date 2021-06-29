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
public class AggMapTs extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private int count = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			count = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeInt(count);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text zzdjsj = (Text)args[2];
		Text zjdjsj = (Text)args[3];
		Text djcs = (Text)args[4];
		Text lybmc = (Text)args[5];
		

		String xmStr = null;
		if (xm != null ) {
			xmStr = xm.toString();
		}
		String zzdjsjStr = null;
		if (zzdjsj != null ) {
			zzdjsjStr = zzdjsj.toString();
		}
		String zjdjsjStr = null;
		if (zjdjsj != null ) {
			zjdjsjStr = zjdjsj.toString();
		}
		String lybmcStr = null;
		if (lybmc != null ) {
			lybmcStr = lybmc.toString();
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("gmsfhm", gmsfhm.toString());
		jsonObject.put("xm", xmStr);
		jsonObject.put("zzdjsj", zzdjsjStr);
		jsonObject.put("zjdjsj", zjdjsjStr);
		jsonObject.put("djcs", djcs.toString());
		jsonObject.put("lybmc", lybmcStr);
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.count <= 20) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
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
			jsondata = buf.json;
		}
		rel.set(jsondata);
		return rel;
	}

}
