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
public class AggMapJsth extends Aggregator   {

	final String HZ = "户主";
	final String PO = "配偶";
	final String YFM = "岳父母或公婆";
	final String FM = "父母";
	
	private static class JsonBuffer implements Writable {
		private String json = "";

		private String ma_addr = "";
		private String fa_addr = "";
		private String live_addr = "";
		
		private int count = 0;
		private int hzCount = 0;
		private int poCount = 0;
		private int yfmCount = 0;
		private int fmCount = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			ma_addr = in.readUTF();
			fa_addr = in.readUTF();
			live_addr = in.readUTF();
			count = in.readInt();
			hzCount = in.readInt();
			poCount = in.readInt();
			yfmCount = in.readInt();
			fmCount = in.readInt();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(ma_addr);
			out.writeUTF(fa_addr);
			out.writeUTF(live_addr);
			out.writeInt(count);
			out.writeInt(hzCount);
			out.writeInt(poCount);
			out.writeInt(yfmCount);
			out.writeInt(fmCount);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text gmsfhm = (Text)args[0];
		Text xm = (Text)args[1];
		Text yhzgx = (Text)args[2];
		Text ma_addr = (Text)args[3];
		Text fa_addr = (Text)args[4];
		Text live_addr = (Text)args[5];
		
		
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
		if (buf.count <= 10 && buf.hzCount <= 1 && buf.poCount <= 1 && buf.fmCount <= 2 && buf.yfmCount <= 2) {
			buf.json = buf.json + "##" + JSONObject.toJSONString(jsonObject);
			if (buf.ma_addr.length() == 0 && ma_addr != null) {
				buf.ma_addr = ma_addr.toString();
			}
			if (buf.fa_addr.length() == 0 && fa_addr != null) {
				buf.fa_addr = fa_addr.toString();
			}
			if (buf.live_addr.length() == 0 && live_addr != null) {
				buf.live_addr = live_addr.toString();
			}
			if (HZ.equals(yhzgxStr)) {
				buf.hzCount += 1;
			}
			if (PO.equals(yhzgxStr)) {
				buf.poCount += 1;
			}
			if (FM.equals(yhzgxStr)) {
				buf.fmCount += 1;
			}
			if (YFM.equals(yhzgxStr)) {
				buf.yfmCount += 1;
			}
			buf.count += 1;
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (buf.count + p.count <= 10 && buf.hzCount + p.hzCount <= 1 && buf.poCount + p.poCount <= 1 &&
				buf.fmCount + p.fmCount <= 2 && buf.yfmCount + p.yfmCount <= 2) {
			buf.json = buf.json + p.json;
			if (buf.ma_addr.length() == 0 && p.ma_addr.length() > 0) {
				buf.ma_addr = p.ma_addr.toString();
			}
			if (buf.fa_addr.length() == 0 && p.fa_addr.length() > 0) {
				buf.fa_addr = p.fa_addr.toString();
			}
			if (buf.live_addr.length() == 0 && p.live_addr.length() > 0) {
				buf.live_addr = p.live_addr.toString();
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
		if (buf.count <= 10 && buf.hzCount <= 1 && buf.poCount <= 1 && buf.fmCount <= 2 && buf.yfmCount <= 2) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("json", buf.json);
			jsonObject.put("ma_addr", buf.ma_addr);
			jsonObject.put("fa_addr", buf.fa_addr);
			jsonObject.put("live_addr", buf.live_addr);
			jsondata = JSONObject.toJSONString(jsonObject);
		}
		rel.set(jsondata);
		return rel;
	}

}
