package com.chinaoly.udtf.split;

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SplitHctcx extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		String history_data = (String) arg0[0];
		if (history_data != null) {
			String[] dataArr =  history_data.trim().split("##");
			for (String data : dataArr) {
				try {
					JSONObject jsonObject = JSONObject.parseObject(data);
					forward(jsonObject.getString("gmsfhm"), jsonObject.getString("xm"), jsonObject.getString("gxrgmsfhm"), jsonObject.getString("gxrxm"), 
						jsonObject.getString("cc"), jsonObject.getString("fcsj"), jsonObject.getString("sfz"), jsonObject.getString("mdz"), 
						jsonObject.getString("cxh"));
				} catch (Exception e) {
				}
			}
		}
	}
	
	
	
	
}
