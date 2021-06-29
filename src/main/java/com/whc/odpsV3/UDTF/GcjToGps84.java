package com.whc.odpsV3;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.IOException;

@Resolve("double,double->double,double")
public class GcjToGps84 extends UDTF {

    @Override
    public void process(Object[] objects) throws UDFException, IOException {

    }
}
