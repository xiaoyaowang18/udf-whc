package com.whc.relation_udf.udtf;

import java.io.IOException;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

public class SortColumns extends UDTF{

	@Override
	public void process(Object[] arg0) throws UDFException, IOException {
		// TODO Auto-generated method stub
		//假设需要换位置的字段个数有3+3一共6个，还有6个不用换位置的字段，一共12个字段。
		//得到输出结果数组长度13-1等于12
		int objectLength = arg0.length - 1;
		Object[] object = new Object[objectLength];
		//获取最后一个字段值，记录了需要换位置的字段个数的一半3
		int length = Integer.parseInt((String) arg0[objectLength]);
		
		//得到两个用来比较用的字段值，分别是下标是0和3的。
		String gmsfhm  = (String) arg0[0];
		String gxrgmsfhm  = (String) arg0[length];
		
		//若gmsfhm比gxrgmsfhm小,则直接将前面12个字段赋值给结果object
		if (gmsfhm.compareTo(gxrgmsfhm) < 0) {
			//0-0,1-1,2-2,3-3,4-4,5-5,6-6,...,11-11
			for (int i = 0; i < objectLength; i++) {
				object[i] = String.valueOf(arg0[i]);
			}
		//否则，将下标为3,4,5的字段赋值给结果object的0,1,2位置,将下标为0,1,2的字段赋值给结果object的3,4,5位置，再将下标为6到11的字段赋值给结果object的6到11位置
		} else {
			//0-3,1-4,2-5
			for (int i = 0; i < length; i++) {
				object[i] = arg0[i + length];
			}
			//3-0,4-1,5-2
			for (int i = length; i < length + length; i++) {
				object[i] = arg0[i - length];
			}
			//6-6,...,11-11
			for (int i = length + length; i < objectLength; i++) {
				object[i] = String.valueOf(arg0[i]);
			}
		}
		//输出结果
		forward(object);
		
	}
	
}
