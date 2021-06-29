package com.chinaoly.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortTjs extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String p1_gmsfhm = (String) arg0[0];
		String p1_xm = (String) arg0[1];
		String p1_kssj = (String) arg0[2];
		String p1_jssj = (String) arg0[3];
		String p2_gmsfhm = (String) arg0[4];
		String p2_xm = (String) arg0[5];
		String p2_kssj = (String) arg0[6];
		String p2_jssj = (String) arg0[7];
		String jsbh = (String) arg0[8];
		String jsh = (String) arg0[9];
		String jsmc = (String) arg0[10];
		String jslb = (String) arg0[11];
		String jsdz = (String) arg0[12];
		
		if (p1_gmsfhm.compareTo(p2_gmsfhm) < 0) {
			forward(p1_gmsfhm, p1_xm, p1_kssj, p1_jssj, p2_gmsfhm, p2_xm, p2_kssj, p2_jssj, jsbh, jsh, jsmc, jslb, jsdz);
		} else {
			forward(p2_gmsfhm, p2_xm, p2_kssj, p2_jssj, p1_gmsfhm, p1_xm, p1_kssj, p1_jssj, jsbh, jsh, jsmc, jslb, jsdz);
		}
		
	}
	
}

