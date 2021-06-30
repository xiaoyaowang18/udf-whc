package com.whc.odpsV3.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created with IntelliJ IDEA.
 * To change this template use File | Settings | File Templates.
 * create function string_chuli as 'com.chinaoly.qtudf.StringChuLiUDF'
 */
public class HandleStr extends UDF {
    public static void main(String[] args){
        String st = "2";
        String sr = "111";
        HandleStr chuLiUDF = new HandleStr();
        System.out.println(chuLiUDF.evaluate(st,sr));
    }

    private String regEx="[`~!@#$%^&*()+=|{}':;',//[//].<>/?~！@#￥%……&*（）——+|{}【】’；：“”‘。，、？]|\n|\r|\t|null";


    /**
     * input=1时 当字符串为纯汉字时 返回原值 否则返回空
     * input=2时 当字符串为纯数字返回原值 否则返回空
     * input=3时 当字符串为纯数字或纯英文字符或纯英文字符和数字混合时 返回空 否则返回原值
     * input=4时 当字符串为纯特殊字符时 返回空 当字符串包含特殊字符时 特殊字符去除后返回原值 否则返回原值
     * input=5时 当字符串包含数字时 数字去除后返回原值 否则返回原值
     * input=6时 当字符串包含字母时 字母去除后返回原值 否则返回原值
     * 当input手动输入需要过滤的特殊字符时，以空格隔开表示等于所输字符串时返回null 否则返回原值
     * 当input手动输入需要过滤的特殊字符时，以英文逗号隔开表示包含所输字符串时返回null 否则返回原值
     * @param input
     * @param s
     * @return
     */
    public String evaluate(String input,String s){
        if(s!=null){
            s=s.trim();

        }
        if (input!=null){
            input=input.trim();
        }

        if(s==null||"".equals(s)){
            return null;
        }
//        System.out.println(input);
        if(input.matches("[0-9]+")) {
            int i=Integer.parseInt(input);
            switch (i){
                //只保留纯汉字
                case 1:
                    if(s.matches("[\\u4e00-\\u9fa5]+")){
                        return s;
                    } else {
                        return null;
                    }

                    //只保留数字
                case 2:
                    if (s.matches("[0-9]+")){
                        return s;
                    } else {
                        return null;
                    }

                    //只保留字母和数字
                case 3:
                    if(s.matches("[0-9]+")||s.matches("[a-zA-Z]+")||s.matches("[a-zA-Z0-9]+")){
                        return s;
                    }
                    else {
                        return null;
                    }
                    //剔除特殊字符
                case 4:
                    return trueStr(s,regEx);
                //剔除数字
                case 5:
                    return trueStr(s,"[0-9]+");
                //剔除字母
                case 6:
                    return trueStr(s,"[a-zA-Z]+");
                case 7:
                    Pattern pattern= Pattern.compile("[\u4e00-\u9fa5]");
                    Matcher m=pattern.matcher(s);

                    if(m.find()){
                        s=trueStr(s,"[0-9]+");
                        s=trueStr(s,regEx);
                        s=trueStr(s,"[a-zA-Z]+");
                        return s;
                    } else {
                        return null;
                    }
                default:return s;
            }
        }else {
            if (input.equals(s)){
                return null;
            }
            //等于所输字符 所输字符以空格隔开 全部置空
            if(input.contains(" ")){
                String[] split=input.split(" ");
                for (String str:split){
                    if(s.equals(str)){
                        return null;
                    }
                }
            }
            //包含所输字符 所输字符以英文逗号隔开 全部置空
            if(input.contains(",")){
                String[] split=input.split(",");
                for (String str:split){
                    if(s.contains(str)){
                        return null;
                    }
                }
            }
            if (input.contains(";")){
                String[] split=input.split(";");
                for (String str:split){
                    s= trueStr(s,str);
                }
            }
        }
        return s;
    }

    //过滤特殊字符
    public String trueStr(String s,String str){
        Pattern p=Pattern.compile(str);
        Matcher m=p.matcher(s);
        if(m.find()){
            s=m.replaceAll("").trim();
            if ("".equals(s)){
                return null;
            }else {
                return s;
            }
        }
        return s;
    }

}