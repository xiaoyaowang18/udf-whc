package com.whc.odpsV3.UDF;


import org.apache.hadoop.hive.ql.exec.UDF;

//全角转半角
public class Qj_To_Bj extends UDF {
    public String evaluate(String fullWidthStr) {
        if (null == fullWidthStr || fullWidthStr.length() <= 0) {
            return "";
        }
        char[] charArrays = fullWidthStr.toCharArray();
        //对全角字符转换的char数组遍历
        for (int i = 0; i < charArrays.length; ++i) {
            int charIntValue = (int) charArrays[i];
            //如果符合转换关系,将对应下标之间减掉偏移量65248;如果是空格的话,直接做转换
            if (charIntValue >= 65281 && charIntValue <= 65374) {
                charArrays[i] = (char) (charIntValue - 65248);
            } else if (charIntValue == 12288) {
                charArrays[i] = (char) 32;
            }
        }
        StringBuffer sb = new StringBuffer();
        String returndata ="";
        for(int i=0 ; i<charArrays.length; i++){
            returndata = sb.append(charArrays[i]).toString();
        }

        return returndata;
    }
//    public static void main(String[] args) {
//        Qzb fww = new Qzb();
//        String a = "ｓｔｒｉｎｇ";
//        System.out.println(fww.evaluate(a));
//    }
}
