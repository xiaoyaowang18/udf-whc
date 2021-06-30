package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortThcdp extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String gmsfhm = (String) arg0[0];
		String xm = (String) arg0[1];
		String gxrgmsfhm = (String) arg0[2];
		String gxrxm = (String) arg0[3];
		String txrid = (String) arg0[4];
		String cc = (String) arg0[5];
		String ccrq = (String) arg0[6];
		String sfz = (String) arg0[7];
		String mdz = (String) arg0[8];
		String spzd = (String) arg0[9];
		String spfs = (String) arg0[10];
		String spck = (String) arg0[11];
		
		if (gmsfhm.compareTo(gxrgmsfhm) < 0) {
			forward(gmsfhm, xm, gxrgmsfhm, gxrxm, txrid, cc, ccrq, sfz, mdz, spzd, spfs, spck);
		} else {
			forward(gxrgmsfhm, gxrxm, gmsfhm, xm, txrid, cc, ccrq, sfz, mdz, spzd, spfs, spck);
		}
		
	}
	
}
