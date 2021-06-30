package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortTtdxlcrj extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String p1_zjhm = (String) arg0[0];
		String p1_xm = (String) arg0[1];
		String p1_crrqsj = (String) arg0[2];
		String p2_zjhm = (String) arg0[3];
		String p2_xm = (String) arg0[4];
		String p2_crrqsj = (String) arg0[5];
		String crkadm = (String) arg0[6];
		String tdh = (String) arg0[7];
		String jtfsdm = (String) arg0[8];
		String jtgjbs = (String) arg0[9];
		
		if (p1_zjhm.compareTo(p2_zjhm) < 0) {
			forward(p1_zjhm, p1_xm, p1_crrqsj, p2_zjhm, p2_xm, p2_crrqsj, crkadm, tdh, jtfsdm, jtgjbs);
		} else {
			forward(p2_zjhm, p2_xm, p2_crrqsj, p1_zjhm, p1_xm, p1_crrqsj, crkadm, tdh, jtfsdm, jtgjbs);
		}
		
	}
	
}

