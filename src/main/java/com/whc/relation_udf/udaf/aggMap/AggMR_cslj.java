package com.whc.relation_udf.udaf.aggMap;

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

@Resolve({"bigint,string,string,bigint->string"})
public class AggMR_cslj extends Aggregator   {

	final static String SEPARATOR = "##";
	final static String LAST = "last";
	
	private static class JsonBuffer implements Writable {
		
		private String last_data = "";
		private long last_count = 0;
		
		private String data = "";
		private long count = 0;
		//次数限制
		private long limitOfTimes = 0;

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			last_data = in.readUTF();
			last_count = in.readLong();
			
			data = in.readUTF();
			count = in.readLong();

			limitOfTimes = in.readLong();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(last_data);
			out.writeLong(last_count);
			
			out.writeUTF(data);
			out.writeLong(count);
			
			out.writeLong(limitOfTimes);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
	
		final long count = ((LongWritable)args[0]).get();
		final String data = ((Text)args[1]).toString();
		final String type = ((Text)args[2]).toString();
		final long limitOfTimes = ((LongWritable)args[3]).get();
		
		final JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.limitOfTimes == 0) {
			buf.limitOfTimes = limitOfTimes;
		}
		if (type.toString().equals(LAST)) {
			buf.last_data = data;
			buf.last_count = count;
		} else {
			buf.data = buf.data.length() > 0 ? buf.data + SEPARATOR + data : data;
			buf.count += 1;
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		final JsonBuffer buf = (JsonBuffer) buffer;
		final JsonBuffer p = (JsonBuffer) partial;
		if (buf.last_data.length() == 0 && p.last_data.length() > 0) {
			buf.last_data = p.last_data;
		}
		if (buf.last_count == 0 && p.last_count > 0) {
			buf.last_count = p.last_count;
		}
		if (p.data.length() > 0) {
			buf.data = buf.data.length() > 0 ? buf.data + SEPARATOR + p.data : p.data;
		}
		if (p.count > 0) {
			buf.count += p.count;
		}
		if (buf.limitOfTimes == 0 && p.limitOfTimes > 0) {
			buf.limitOfTimes = p.limitOfTimes;
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
		String result_data = buf.data;
		
		//若历史次数小于次数限制，说明历史数据还没有同步到次数限制表，如果历史数据有值，需要将历史数据利用起来
		if (buf.last_count < buf.limitOfTimes) {
			//若历史数据和新数据都有值，则拼接历史数据和新数据赋值给变量
			if (buf.last_data.length() > 0 && result_data.length() > 0) {
				result_data = buf.last_data + SEPARATOR + result_data;
			//若只有历史数据有值，则将历史数据赋值给变量
			} else if (buf.last_data.length() > 0) {
				result_data = buf.last_data;
			}//最后一种情况是只有新数据有值，则无需赋值
		}
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("history_data", result_data);
		jsonObject.put("count", buf.count + buf.last_count);
		jsonObject.put("last_count", buf.last_count);
		
		rel.set(JSON.toJSONString(jsonObject));
		return rel;
	}

}
