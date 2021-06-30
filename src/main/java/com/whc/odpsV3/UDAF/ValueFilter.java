package com.whc.udaf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({ "string,string,string->string" })
public class ValueFilter extends Aggregator {

	private static class ParaBuffer implements Writable {
		private String para = "";
		private int gxsj = 0;
		private int bxh = 500;

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			para = in.readUTF();
			gxsj = in.readInt();
			bxh = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(para);
			out.writeInt(gxsj);
			out.writeInt(bxh);
		}
	}

	private Text rel = new Text();
	
	@Override
	public void iterate(Writable buffer, Writable[] args) throws UDFException {
		// TODO Auto-generated method stub
		Text paraT = (Text) args[0];
		Text gxsjT = (Text) args[1];
		Text bxhT = (Text) args[2];

		ParaBuffer buf = (ParaBuffer) buffer;

		String paraStr = null;
		int gxsjStr = 0;

		// 对传进来的值进行转换处理
		K: if (paraT != null) {
			paraStr = paraT.toString();

			if (gxsjT != null)
				gxsjStr = Integer.parseInt(gxsjT.toString());

			int bxhStr = Integer.parseInt(bxhT.toString());

			// 标序号为1、2、3、4的这几张表值优先更新，不考虑其他表
//			System.out.println("清洗过程："+paraStr+"  "+gxsjStr+"  "+bxhStr);
			switch (bxhStr) {
				case 1:
					if (buf.bxh > bxhStr || buf.gxsj <= gxsjStr) {
							buf.para = paraStr;
							buf.bxh = bxhStr;
							buf.gxsj = gxsjStr;
							break K;
					}
				case 2:
					if (buf.bxh>1 && (buf.bxh > bxhStr || buf.gxsj <= gxsjStr)) {
						buf.para = paraStr;
						buf.bxh = bxhStr;
						buf.gxsj = gxsjStr;
						break K;
					}
				case 3:
					if (buf.bxh>2 && (buf.bxh > bxhStr || buf.gxsj <= gxsjStr)) {
						buf.para = paraStr;
						buf.bxh = bxhStr;
						buf.gxsj = gxsjStr;
						break K;
					}
				case 4:
					if (buf.bxh>3 && (buf.bxh > bxhStr || buf.gxsj <= gxsjStr)) {
						buf.para = paraStr;
						buf.bxh = bxhStr;
						buf.gxsj = gxsjStr;
						break K;
					}
			}

			// 更新时间不为空
			if (gxsjStr != 0 && buf.gxsj < gxsjStr && buf.bxh >= bxhStr && bxhStr>4) {
				buf.para = paraStr;
				buf.bxh = bxhStr;
				buf.gxsj = gxsjStr;
			}

			// 更新时间为空的情况下
			else if (gxsjStr == 0 && buf.bxh > bxhStr && bxhStr>4) {
				buf.para = paraStr;
				buf.bxh = bxhStr;
				buf.gxsj = gxsjStr;
			}
//			System.out.println("清洗结果："+buf.para+"  "+buf.gxsj+"  "+buf.bxh);
		}
	}

	@Override
	public void merge(Writable buffer, Writable partial) throws UDFException {
		// TODO Auto-generated method stub
		// merge合并过程
		ParaBuffer buf = (ParaBuffer) buffer;
		ParaBuffer p = (ParaBuffer) partial;
		
		// 标序号为1、2、3、4的这几张表值优先更新，不考虑其他表
		S: if(p.para.length() > 0) {
//			System.out.println("合并过程："+p.para+"  "+p.gxsj+"  "+p.bxh);
			switch (p.bxh) {
				case 1:
					if (buf.bxh > p.bxh || buf.gxsj <= p.gxsj) {
						buf.para = p.para;
						buf.bxh = p.bxh;
						buf.gxsj = p.gxsj;
						break S;
					}
				case 2:
					if (buf.bxh>1 && (buf.bxh > p.bxh || buf.gxsj <= p.gxsj) ) {
						buf.para = p.para;
						buf.bxh = p.bxh;
						buf.gxsj = p.gxsj;
						break S;
					}
				case 3:
					if (buf.bxh>2 && (buf.bxh > p.bxh || buf.gxsj <= p.gxsj)) {
						buf.para = p.para;
						buf.bxh = p.bxh;
						buf.gxsj = p.gxsj;
						break S;
					}
				case 4:
					if (buf.bxh>3 && (buf.bxh > p.bxh || buf.gxsj <= p.gxsj)) {
						buf.para = p.para;
						buf.bxh = p.bxh;
						buf.gxsj = p.gxsj;
						break S;
					}
			}
			
			// 更新时间不为空
			if (p.gxsj != 0 && buf.bxh >p.bxh && buf.gxsj < p.gxsj && p.bxh>4) {
				buf.para = p.para;
				buf.bxh = p.bxh;
				buf.gxsj = p.gxsj;
			}

			// 更新时间为空的情况下
			else if (buf.bxh >p.bxh && p.gxsj == 0 && p.bxh>4) {
				buf.para = p.para;
				buf.bxh = p.bxh;
				buf.gxsj = p.gxsj;
			}

//			System.out.println("合并结果："+buf.para+"  "+buf.gxsj+"  "+buf.bxh);
		}
	}

	@Override
	public Writable newBuffer() {
		// TODO Auto-generated method stub
		return new ParaBuffer();
	}

	@Override
	public Writable terminate(Writable buffer) throws UDFException {
		ParaBuffer buf = (ParaBuffer) buffer;
		if (buf.para.length()>0) {
			rel.set(buf.para);
			return rel;
		}
		return null;
	}
}
