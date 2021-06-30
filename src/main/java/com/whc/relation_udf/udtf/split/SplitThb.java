package com.whc.relation_udf.udtf.split;

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SplitThb extends UDTF{

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
						jsonObject.getString("hbh"), jsonObject.getString("hbrq"), jsonObject.getString("lgsj"), jsonObject.getString("dgsj"), 
						jsonObject.getString("sfd"), jsonObject.getString("mdd"));
				} catch (Exception e) {
				}
			}
		}
	}
	
	
	
	
}
