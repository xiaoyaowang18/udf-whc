package com.chinaoly.udtf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class GetJsonResult_cslj extends UDTF {
	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		
		//假设一共有13个参数，arg0下标0和1是两个公民身份号码
		
		String result = "请传入正确参数";
		final String gmsfhm = (String) arg0[0];
		final String gxrgmsfhm = (String) arg0[1];
		//arg0下标2是字段名称
		final String columns = (String) arg0[2];
		
		final String key = StringUtils.substring(gmsfhm, 17, 18) + StringUtils.substring(gxrgmsfhm, 17, 18) + gmsfhm + gxrgmsfhm;
		
		if (columns != null) {
			final String[] columnArr = columns.split(",");
			final int length =  columnArr.length;			//9
			if ((length) != arg0.length - 3) {		
				result = "前后字段数量不一致";
			} else {
				final JSONObject jsonObject = new JSONObject();
				//arg0下标3到11对应md5ColumnArr下标0到8
				for (int i = 0; i < length; i++) {
					jsonObject.put(columnArr[i].trim(), (String) arg0[i + 3]);
				}
				result = JSON.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue);
			}
		} 

		forward(key, result);	
		
	}
	
	
	
	
}
