package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortJgts extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String gmsfhm = (String) arg0[0];
		String xm = (String) arg0[1];
		String hjrgmsfhm = (String) arg0[2];
		String hjrxm = (String) arg0[3];
		String kssj = (String) arg0[4];
		
		if (gmsfhm.compareTo(hjrgmsfhm) < 0) {
			forward(gmsfhm, xm, hjrgmsfhm, hjrxm, kssj);
		} else {
			forward(hjrgmsfhm, hjrxm, gmsfhm, xm, kssj);
		}
		
	}
	
}
