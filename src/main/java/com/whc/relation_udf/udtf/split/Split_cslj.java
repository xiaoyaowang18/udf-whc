package com.chinaoly.udtf.split;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class Split_cslj extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		String json = (String) arg0[0];
		String key = (String) arg0[1];
		
		if (json != null) {
			
				JSONObject jsonObject = JSON.parseObject(json);
				forward(key, StringUtils.substring(key, 2, 20), StringUtils.substring(key, 20, 38), String.valueOf(jsonObject.getLong("count")), 
						String.valueOf(jsonObject.getLong("last_count")), jsonObject.getString("history_data"));
			
		}
	}
	
}
