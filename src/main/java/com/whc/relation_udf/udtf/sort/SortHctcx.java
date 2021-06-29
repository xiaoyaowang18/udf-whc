package com.chinaoly.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortHctcx extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String gmsfhm = (String) arg0[0];
		String xm = (String) arg0[1];
		String gxrgmsfhm = (String) arg0[2];
		String gxrxm = (String) arg0[3];
		String cc = (String) arg0[4];
		String fcsj = (String) arg0[5];
		String sfz = (String) arg0[6];
		String mdz = (String) arg0[7];
		String cxh = (String) arg0[8];
		
		if (gmsfhm.compareTo(gxrgmsfhm) < 0) {
			forward(gmsfhm, xm, gxrgmsfhm, gxrxm, cc, fcsj, sfz, mdz, cxh);
		} else {
			forward(gxrgmsfhm, gxrxm, gmsfhm, xm, cc, fcsj, sfz, mdz, cxh);
		}
		
	}
	
}
