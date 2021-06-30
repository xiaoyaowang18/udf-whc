package com.whc.relation_udf.udaf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string,string,string,string,string->string"})
public class AggFilter extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String detail = "";
		private String lgsj = "";

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			detail = in.readUTF();
			lgsj = in.readUTF();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(detail);
			out.writeUTF(lgsj);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text hbh = (Text)args[0];
		Text hbrq = (Text)args[1];
		Text lgsj = (Text)args[2];
		Text dgsj = (Text)args[3];
		Text sfd = (Text)args[4];
		Text mdd = (Text)args[5];
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (buf.lgsj.compareTo(lgsj.toString()) < 0) {
			buf.detail = "hbh:" + hbh + ",hbrq:" + hbrq + ",lgsj:" + lgsj
					+ ",dgsj:" + dgsj + ",sfd:" + sfd + ",mdd:" + mdd;
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (buf.lgsj.compareTo(p.lgsj) < 0) {
			buf.lgsj = p.lgsj;
			buf.detail = p.detail;
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
		rel.set(buf.detail);
		return rel;
	}

}
