package com.whc.odpsV3.UDF;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Get_Xm extends UDF {

    public String evaluate(String xm) {
            String reg = "[^一-龥]";
            return StringUtils.isNotEmpty(xm) ? xm.trim().replaceAll(reg, "") : null;
    }
}
