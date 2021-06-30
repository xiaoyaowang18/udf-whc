package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

/**
 * 比较双方身份证号，小的在前，
 * 以gmsfhm, xm, pogmsfhm, poxm, gxlx, djsj, sjy字段数据输出
 *
 * 输入gmsfhm, xm, pogmsfhm, poxm, gxlx, djsj, sjy
 * 输出
 */
public class SortPo extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String gmsfhm = (String) arg0[0];
		String xm = (String) arg0[1];
		String pogmsfhm = (String) arg0[2];
		String poxm = (String) arg0[3];
		String djsj = (String) arg0[4];
		String gxlx = (String) arg0[5];
		String sjy = (String) arg0[6];
		
		if (gmsfhm.compareTo(pogmsfhm) < 0) {
			forward(gmsfhm, xm, pogmsfhm, poxm, gxlx, djsj, sjy);
		} else {
			forward(pogmsfhm, poxm, gmsfhm, xm, gxlx, djsj, sjy);
		}
		
	}
	
}
