package com.chinaoly.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortWbtsw extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String p1_zjhm = (String) arg0[0];
		String p1_xm = (String) arg0[1];
		String p1_kssj = (String) arg0[2];
		String p1_jssj = (String) arg0[3];
		String p2_zjhm = (String) arg0[4];
		String p2_xm = (String) arg0[5];
		String p2_kssj = (String) arg0[6];
		String p2_jssj = (String) arg0[7];
		String wbdm = (String) arg0[8];
		String wbmc = (String) arg0[9];
		String wbdz = (String) arg0[10];
		String xzqh = (String) arg0[11];
		
		if (p1_zjhm.compareTo(p2_zjhm) < 0) {
			forward(p1_zjhm, p1_xm, p1_kssj, p1_jssj, p2_zjhm, p2_xm, p2_kssj, p2_jssj, wbdm, wbmc, wbdz, xzqh);
		} else {
			forward(p2_zjhm, p2_xm, p2_kssj, p2_jssj, p1_zjhm, p1_xm, p1_kssj, p1_jssj, wbdm, wbmc, wbdz, xzqh);
		}
		
	}
	
}

