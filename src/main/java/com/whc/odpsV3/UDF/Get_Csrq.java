package com.whc.odpsV3.UDF;

import org.apache.commons.lang3.StringUtils;

//从身份证号中获取到出生日期
public class Get_Csrq {
    public Get_Csrq() {
    }

    public String evaluate(String sfzm) {
        String returndata =!StringUtils.isEmpty(sfzm) && StringUtils.length(sfzm) == 18 ? StringUtils.substring(sfzm, 6, 14) : null;
        return returndata;
    }
}
