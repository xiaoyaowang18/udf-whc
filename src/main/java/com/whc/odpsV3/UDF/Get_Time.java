package com.whc.odpsV3.UDF;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Get_Time extends UDF {
    public Get_Time() {
    }

    public String evaluate(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        } else {
            str = str.trim();
            String string = "";

            for(int i = 0; i < str.length(); ++i) {
                if (str.charAt(i) >= '0' && str.charAt(i) <= '9') {
                    string = string + str.charAt(i);
                }
            }

            if (string.length() < 8) {
                return null;
            } else if (string.length() >= 8 && string.length() < 14) {
                return string.substring(0, 8) + "000000";
            } else {
                return string.substring(0, 14);
            }
        }
    }

//    public static void main(String[] args) {
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
//        String time = df.format(new Date());
//        System.out.println(time);// new Date()为获取当前系统时间
//          Get_Time gt = new Get_Time();
//        System.out.println(gt.evaluate(time));
//    }

}
