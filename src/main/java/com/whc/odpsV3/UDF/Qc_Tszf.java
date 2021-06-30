package com.whc.odpsV3.UDF;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

//去除特殊字符
public class Qc_Tszf extends UDF {
    public Qc_Tszf() {
    }

    public String evaluate(String str) {
        str = this.tocdb(str);
        if (StringUtils.isEmpty(str)) {
            return null;
        } else {
            str = str.replace("\t", "");
            str = str.replace("\n", "");
            str = str.replace("\r", "");
            str = str.replace(" ", "");
            str = str.replace("　", "");
            str = str.replace("空", "");
            str = str.replace("暂无是", "");
            str = str.replace("缺失", "");
            str = str.replace("null", "");
            str = str.replace("手工录入", "");
            str = str.replace("未知字段", "");
            str = str.replace("手工录入和未知字段", "");
            str = str.replace("()", "");
            if(str.length() == 5 && str.contains("无宗教信仰")){

            }else{
                str = str.replace("无", "");
            }

            return StringUtils.isEmpty(str) ? null : str;
        }
    }

    public String tocdb(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        } else {
            String tmp = "";

            for(int i = 0; i < str.length(); ++i) {
                if (str.charAt(i) > 'ﻠ' && str.charAt(i) < '｟') {
                    tmp = tmp + (char)(str.charAt(i) - 'ﻠ');
                } else {
                    tmp = tmp + str.charAt(i);
                }
            }

            return tmp;
        }
    }

//    public static void main(String[] args) {
//          Qc_Tszf ao = new Qc_Tszf();
//          System.out.println(ao.evaluate("12315(6)无1"));
//          String a = "无宗教信仰";
//          System.out.println(a.length());
//    }
}
