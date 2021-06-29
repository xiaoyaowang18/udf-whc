package com.chinaoly.udaf.aggMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"string,string->string"})
public class AggMapFlagLsthk extends Aggregator   {

	private static class JsonBuffer implements Writable {
		private String json = "";
		
		private String relationType = "";

		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			json = in.readUTF();
			relationType = in.readUTF();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(json);
			out.writeUTF(relationType);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text relationType = (Text)args[0];
		Text json = (Text)args[1];
		
		JsonBuffer buf = (JsonBuffer) buffer;
		if (!(buf.relationType.equals("thk") && relationType.toString().equals("lsthk"))) {
			buf.json = json.toString();
			buf.relationType = relationType.toString();
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		JsonBuffer buf = (JsonBuffer) buffer;
		JsonBuffer p = (JsonBuffer) partial;
		if (!(buf.relationType.equals("thk") && p.relationType.equals("lsthk"))) {
			buf.json = p.json.toString();
			buf.relationType = p.relationType.toString();
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
		rel.set(buf.relationType + "##" + buf.json);
		return rel;
	}

}
