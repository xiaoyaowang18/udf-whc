package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortThk extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String p1_zjhm = (String) arg0[0];
		String p1_xm = (String) arg0[1];
		String p1_yhzgx = (String) arg0[2];
		String p1_djsj = (String) arg0[3];
		String p2_zjhm = (String) arg0[4];
		String p2_xm = (String) arg0[5];
		String p2_yhzgx = (String) arg0[6];
		String p2_djsj = (String) arg0[7];
		String czrkhh = (String) arg0[8];
		String czrkhlx = (String) arg0[9];
		String czrkzz = (String) arg0[10];
		String czrkssssxq = (String) arg0[11];
		
		if (p1_zjhm.compareTo(p2_zjhm) < 0) {
			forward(p1_zjhm, p1_xm, p1_yhzgx, p1_djsj, p2_zjhm, p2_xm, p2_yhzgx, p2_djsj, czrkhh, czrkhlx, czrkzz, czrkssssxq);
		} else {
			forward(p2_zjhm, p2_xm, p2_yhzgx, p2_djsj, p1_zjhm, p1_xm, p1_yhzgx, p1_djsj, czrkhh, czrkhlx, czrkzz, czrkssssxq);
		}
		
	}
	
}

