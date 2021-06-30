package com.whc.relation_udf.udtf;

import java.io.IOException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SplitJsonColumns extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		String detail = (String) arg0[0];
		String columns = (String) arg0[1];
		String defaultValue = (String) arg0[2];
		int originalColumnCount = arg0.length - 3;

		
		if (detail != null) {
			String[] detailArr = detail.split("##");
			final String[] columnArr = columns.split(",");
			final int length =  columnArr.length;	
			for (String json : detailArr) {
				JSONObject jsonObject = JSON.parseObject(json);		
				Object[] object = new Object[originalColumnCount + length];
				for (int i = 0; i < originalColumnCount; i++) {
					object[i] = String.valueOf(arg0[i + 3]);
				}
				
				if ((length) == jsonObject.size()) {
					for (int i = 0; i < length; i++) {
						object[i + originalColumnCount] = jsonObject.get(columnArr[i].trim());
					}
					forward(object);
				} else {
					for (int i = 0; i < length; i++) {
						object[i + originalColumnCount] = 
							jsonObject.get(columnArr[i].trim()) == null ? defaultValue : jsonObject.get(columnArr[i].trim());
					}
					forward(object);
				}
			}
		}
	}
}
