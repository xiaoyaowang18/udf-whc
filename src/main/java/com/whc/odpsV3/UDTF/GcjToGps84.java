package com.whc.odpsV3.UDTF;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.whc.tools.PositionUtil;

import java.io.IOException;

@Resolve("string,string->double,double")
public class GcjToGps84 extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException, IOException {
        String arg1 = (String) args[0];
        String arg2 = (String) args[1];
        double jd = Double.parseDouble(arg1);
        double wd = Double.parseDouble(arg2);

        double[] doubles = PositionUtil.gcj02_To_Gps84(wd, jd);
        forward(doubles[1], doubles[0]);
    }
}
