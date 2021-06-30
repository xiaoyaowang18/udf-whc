package com.whc.odpsV3.UDF;

import com.aliyun.odps.udf.UDF;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Check_Dz extends UDF {

    private static final Pattern p = Pattern.compile("[\u4e00-\u9fa5]");

    public String evaluate(String str) {
        if (StringUtils.isNotEmpty(str)) {
            if (isContainsChinese(str)) {
                String reg = "[^A-Za-z0-9\u4e00-\u9fa5()（）、，,：]";
                str = str.trim().replaceAll(reg, "");
                Matcher m = p.matcher(str);
                if (m.find()) {
                    str = str.substring(m.start(), str.length());
                }
                return str;
            }
        }
        return null;
    }

    private boolean isContainsChinese(String str) {
        Matcher m = p.matcher(str);
        if (m.find()) return true;
        return false;
    }

    public static void main(String[] args) {
        Check_Dz c = new Check_Dz();
        System.out.println(c.evaluate("5478答复42结果就是3422se,rgetg"));
        System.out.println(c.evaluate("答复547dsf"));
    }
}
