package com.whc.relation_udf.udtf.reduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class ReduceFlagLsthk extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		String data = (String) arg0[0];
		String gmsfhm = (String) arg0[1];
		String gxrgmsfhm = (String) arg0[2];
		String relationType = StringUtils.substringBefore(data, "##");
		String jsondata = StringUtils.substringAfter(data, "##");
		try {
			JSONObject json = JSON.parseObject(jsondata);
			forward(gmsfhm, json.getString("xm"), json.getString("yhzgx"), json.getString("djsj"), gxrgmsfhm, json.getString("gxrxm"), json.getString("gxryhzgx")
					, json.getString("gxrdjsj"), json.getString("hh"), json.getString("hlx"), json.getString("zz"), json.getString("xzqhmc"), relationType);						
		} catch(Exception e) {
			
		}
	}
}
