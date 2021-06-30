package com.whc.odpsV3.UDF;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.udf.UDF;
import com.whc.tools.PositionUtil;

public class ToGps84 extends UDF {
    private Text ret = new Text();

    public double[] evalute(Double a, Double b) {
        if (a == null || b == null) {
            return null;
        }

        ret.clear();

        return PositionUtil.gcj02_To_Gps84(b,a);
    }

    public static void main(String[] args) {
        ToGps84 toGps84 = new ToGps84();
        double[] evalute = toGps84.evalute(121.37176399362419,28.58457644890437);
        System.out.println(evalute[1]+" "+evalute[0]);
    }
}


