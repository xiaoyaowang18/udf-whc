package com.whc.odpsV3.UDF;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Get_Py extends UDF {
    public Get_Py() {
    }

    public String evaluate(String str) {
        if (StringUtils.isNotEmpty(str)) {
            String reg = "[^a-zA-Z]";
            return str.trim().replaceAll(reg, "");
        } else {
            return null;
        }
    }
}
