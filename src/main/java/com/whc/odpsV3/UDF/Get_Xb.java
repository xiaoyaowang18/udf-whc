package com.whc.odpsV3.UDF;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Get_Xb extends UDF {
    public static String evaluate(String sfzm) {
        if (!StringUtils.isEmpty(sfzm) && StringUtils.length(sfzm) == 18) {
            return Integer.parseInt(StringUtils.substring(sfzm, 16, 17)) % 2 == 0 ? "女" : "男";
        } else {
            return "未知的性别";
        }
    }

    public static void main(String[] args) {
        System.out.println(evaluate("33090319950410511X"));
    }

}
