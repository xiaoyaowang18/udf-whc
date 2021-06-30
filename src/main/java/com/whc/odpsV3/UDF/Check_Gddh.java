package com.whc.odpsV3.UDF;


import com.whc.tools.Tool;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 验证座机合规性,不合规的座机统一为null;
 * @author Administrator
 *
 */
public class Check_Gddh extends UDF{
    public String evaluate(String zjhm) {
        String zjhm_format = "";
        if(zjhm!=null) {
            if(Tool.verifyIsDhhm(zjhm)) {
                zjhm_format=zjhm;
            }
        }
        return zjhm_format;
    }



    public static void main(String[] args) {
        Check_Gddh test=new Check_Gddh();
//        System.out.println(test.evaluate("0396-2235647"));
    }
}

