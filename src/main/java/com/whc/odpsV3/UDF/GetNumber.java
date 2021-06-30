package com.whc.odpsV3.UDF;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class GetNumber extends UDF{
        public GetNumber() {
        }

        public String evaluate(String str) {
            if (StringUtils.isNotEmpty(str)) {
                String reg = "[^0-9]";
                String num = str.trim().replaceAll(reg, "");
                return num;
            } else {

                return null;
            }
        }

//    public static void main(String[] args) {
//        GetNumber gn = new GetNumber();
//        gn.evaluate("你好132你好ga");
//    }

}

