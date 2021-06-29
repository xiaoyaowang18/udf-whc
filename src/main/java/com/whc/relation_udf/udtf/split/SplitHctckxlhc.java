package com.chinaoly.udtf.split;

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SplitHctckxlhc extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		String history_data = (String) arg0[0];
		if (history_data != null) {
			String[] dataArr =  history_data.trim().split("##");
			for (String data : dataArr) {
				try {
					JSONObject jsonObject = JSONObject.parseObject(data);
					forward(jsonObject.getString("gmsfhm"), jsonObject.getString("xm"), jsonObject.getString("hcsj"), jsonObject.getString("gxrgmsfhm"), 
						jsonObject.getString("gxrxm"), jsonObject.getString("gxrhcsj"), jsonObject.getString("cz"), jsonObject.getString("hcck"));
				} catch (Exception e) {
				}
			}
		}
	}
	
	
	
	
}
