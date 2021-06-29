package com.chinaoly.udtf.split;

import java.io.IOException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SplitHclph extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		String json = (String) arg0[0];
		String cc = (String) arg0[1];
		String ccrq = (String) arg0[2];
		String qsz = (String) arg0[3];
		String mdz = (String) arg0[4];
		if (json != null) {
			JSONArray jsonArray = JSON.parseArray(json);
			for (int i = 0; i < jsonArray.size(); i++) {
				try {
					JSONObject jsonObject = jsonArray.getJSONObject(i);
					forward(jsonObject.getString("gmsfhm"), jsonObject.getString("xm"), jsonObject.getString("ph"), jsonObject.getString("gxrgmsfhm"), 
						jsonObject.getString("gxrxm"), jsonObject.getString("gxrph"), cc, ccrq, qsz, mdz);
				} catch (Exception e) {
				}
			}
		}
	}
	
	
	
	
}
