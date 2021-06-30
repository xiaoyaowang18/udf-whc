package com.whc.relation_udf.udtf.sort;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortTckxlhc extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String p1_zjhm = (String) arg0[0];
		String p1_xm = (String) arg0[1];
		String p1_checkin_time = (String) arg0[2];
		String p2_zjhm = (String) arg0[3];
		String p2_xm = (String) arg0[4];
		String p2_checkin_time = (String) arg0[5];
		String station_code_mc = (String) arg0[6];
		String checkin_window = (String) arg0[7];
		
		if (p1_zjhm.compareTo(p2_zjhm) < 0) {
			forward(p1_zjhm, p1_xm, p1_checkin_time, p2_zjhm, p2_xm, p2_checkin_time, station_code_mc, checkin_window);
		} else {
			forward(p2_zjhm, p2_xm, p2_checkin_time, p1_zjhm, p1_xm, p1_checkin_time, station_code_mc, checkin_window);
		}
		
	}
	
}

