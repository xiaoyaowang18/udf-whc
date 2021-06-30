package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortLgtzs extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String p1_zjhm = (String) arg0[0];
		String p1_xm = (String) arg0[1];
		String p1_rzsj = (String) arg0[2];
		String p1_ldsj = (String) arg0[3];
		String p2_zjhm = (String) arg0[4];
		String p2_xm = (String) arg0[5];
		String p2_rzsj = (String) arg0[6];
		String p2_ldsj = (String) arg0[7];
		String lgdm = (String) arg0[8];
		String lgmc = (String) arg0[9];
		String lgdz = (String) arg0[10];
		String xzqh = (String) arg0[11];
		
		if (p1_zjhm.compareTo(p2_zjhm) < 0) {
			forward(p1_zjhm, p1_xm, p1_rzsj, p1_ldsj, p2_zjhm, p2_xm, p2_rzsj, p2_ldsj, lgdm, lgmc, lgdz, xzqh);
		} else {
			forward(p2_zjhm, p2_xm, p2_rzsj, p2_ldsj, p1_zjhm, p1_xm, p1_rzsj, p1_ldsj, lgdm, lgmc, lgdz, xzqh);
		}
		
	}
	
}

