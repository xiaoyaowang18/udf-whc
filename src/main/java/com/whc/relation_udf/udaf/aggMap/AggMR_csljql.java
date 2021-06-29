package com.chinaoly.udaf.aggMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string,bigint->string"})
public class AggMR_csljql extends Aggregator   {

	final static String SEPARATOR = "##";
		
	private static class JsonBuffer implements Writable {
		
		private String last_data = "";
		
		private String data = "";
		private long count = 0;
		//次数限制
		private long limitOfTimes = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			last_data = in.readUTF();
			
			data = in.readUTF();
			count = in.readLong();

			limitOfTimes = in.readLong();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(last_data);
			
			out.writeUTF(data);
			out.writeLong(count);
			
			out.writeLong(limitOfTimes);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
	
		final String data = ((Text)args[0]).toString();
		final long limitOfTimes = ((LongWritable)args[1]).get();
		
		final JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.limitOfTimes == 0) {
			buf.limitOfTimes = limitOfTimes;
		}
		
		if (buf.count < limitOfTimes) {
			buf.data = buf.data.length() > 0 ? buf.data + SEPARATOR + data : data;
		}
		
		buf.count += 1;
		
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		final JsonBuffer buf = (JsonBuffer) buffer;
		final JsonBuffer p = (JsonBuffer) partial;
		if (buf.limitOfTimes == 0 && p.limitOfTimes > 0) {
			buf.limitOfTimes = p.limitOfTimes;
		}
		if (buf.count + p.count < buf.limitOfTimes) {

			if (p.data.length() > 0) {
				buf.data = buf.data.length() > 0 ? buf.data + SEPARATOR + p.data : p.data;
			}
		}
		if (p.count > 0) {
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
		final JsonBuffer buf = (JsonBuffer) buffer;
		
		
		JSONObject jsonObject = new JSONObject();
		if (buf.count < buf.limitOfTimes) {
			jsonObject.put("history_data", buf.data);
		} else {
			jsonObject.put("history_data", "");
		}
		jsonObject.put("count", buf.count);
		jsonObject.put("last_count", 0l);
		
		rel.set(JSON.toJSONString(jsonObject));
		return rel;
	}

}
