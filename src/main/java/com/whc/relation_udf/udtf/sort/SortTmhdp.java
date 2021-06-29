package com.chinaoly.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortTmhdp extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String gmsfhm = (String) arg0[0];
		String xm = (String) arg0[1];
		String gxrgmsfhm = (String) arg0[2];
		String gxrxm = (String) arg0[3];
		String ddbh = (String) arg0[4];
		String hbh = (String) arg0[5];
		String hbrq = (String) arg0[6];
		String sfd = (String) arg0[7];
		String mdd = (String) arg0[8];
		
		if (gmsfhm.compareTo(gxrgmsfhm) < 0) {
			forward(gmsfhm, xm, gxrgmsfhm, gxrxm, ddbh, hbh, hbrq, sfd, mdd);
		} else {
			forward(gxrgmsfhm, gxrxm, gmsfhm, xm, ddbh, hbh, hbrq, sfd, mdd);
		}
		
	}
	
}
