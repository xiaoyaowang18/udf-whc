package com.whc.odpsV3.UDF;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;


public class Zjhm_To_Gmsfhm extends UDF {
    public Zjhm_To_Gmsfhm() {
    }

    public String evaluate(String zjhm, String zjlx) {
        return !StringUtils.isNotBlank(zjlx) || !StringUtils.isNotBlank(zjhm) || !"居民身份证".equals(zjlx) && !"身份证".equals(zjlx) ? null : zjhm;
    }


    public static void main(String[] args){
        String zjhm = "330105199611237300";
        String zjlx = "居民身份证";
        String zjlx1 = "身份证";
        String zjlx2 = "公民身份号码";
        Zjhm_To_Gmsfhm zjhm_To_Gmsfhm = new Zjhm_To_Gmsfhm();
        System.out.println(zjhm_To_Gmsfhm.evaluate(zjhm,zjlx2));
    }

}
