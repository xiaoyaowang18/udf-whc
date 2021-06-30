package com.whc.relation_udf.udtf;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SplitColumns extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		String detail = (String) arg0[0];
		int originalColumnCount = arg0.length - 1;

		
		if (detail != null) {
			String[] detailArr = detail.split("##");
			for (String info : detailArr) {
				final String[] infoArr = (info + " ").split("::");		
				Object[] object = new Object[originalColumnCount + infoArr.length];
				for (int i = 0; i < originalColumnCount; i++) {
					object[i] = String.valueOf(arg0[i + 1]);
				}
				
				for (int i = 0; i < infoArr.length; i++) {
					object[i + originalColumnCount] = infoArr[i].trim();
				}
				forward(object);
			}
		}
	}
}
