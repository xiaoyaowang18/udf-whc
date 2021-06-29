package com.whc.odpsV3;

import com.aliyun.odps.udf.UDF;
import com.whc.tools.Tool;
import org.apache.commons.lang.StringUtils;

public class Check_Gmsfhm extends UDF {
    public String evaluate(String sfzh) {
        //去除空字符串
        sfzh = StringUtils.trimToEmpty(sfzh);
        //校验
        if (Tool.verifyIsSfzh(sfzh) && !StringUtils.substring(sfzh, 0, 6).equals("111111")) {
            //15位身份证，转换为18位
            sfzh = Tool.getEighteenIDCard(sfzh);
            //验证省份，年份及校验码
            if (sfzh != null && Tool.verifyAreaCode(sfzh) && Tool.verifyBirthday
                    (StringUtils.substring(sfzh, 6, 14)) && Tool.verifyMOD(sfzh)) {
                return sfzh;
            }
        }

        return null;
    }

    public static void main(String[] args) {
        Check_Gmsfhm check_gmsfhm = new Check_Gmsfhm();
        System.out.println(check_gmsfhm.evaluate("120107197104063014"));
    }
}
