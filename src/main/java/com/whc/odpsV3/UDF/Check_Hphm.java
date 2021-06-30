package com.whc.odpsV3.UDF;


import com.whc.tools.Tool;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 验证车牌号合规性,不合规的号码统一为null;
 * @author Administrator
 *
 */

public class Check_Hphm extends UDF{


    public String evaluate(String cphm) {
        String cphm_format = "";
        if(cphm!=null) {
            if(Tool.verifyIsCphm(cphm)) {
                cphm_format=cphm;
            }
        }
        return cphm_format;
    }



    public static void main(String[] args) {
        Check_Hphm test=new Check_Hphm();
        System.out.println(test.evaluate("云A1AN25"));
    }
}
