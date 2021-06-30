package com.whc.odpsV3.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 处理电话号码，将任意字符串中11位电话号码取出来
 * 提取手机号码UDF
 * create function handle_cell as 'com.chinaoly.HandleCell'
 */
public class HandleCell extends UDF {

    //    执行程序入口
    public static void main(String[] args){
        String str = "asdasdsad15737453062-sdadasd15988888888hha0955-1234567";
//        输出已查询到的手机电话号码
        HandleCell phoneNumUDF = new HandleCell();
//        phoneNumUDF.phoneNum(str);

//        System.out.println(phoneNumUDF.evaluate("s0396-6373943asdasdsad15737453062-sdadasd15988888888hha0955-1234567"));
        System.out.println(phoneNumUDF.evaluate("s0396-6373943asdasdsad1573543052-sdad电话号码15688899988手机号啊55-1234567"));

//        输出已查询到的固话号码
//        telpeNum("s0396-6373943asdasdsad15737453062-sdadasd15988888888hha0955-1234567");

    }


    public String evaluate(String s){
        if(s!=null){
            s=s.trim();
        }
        if(s==null||"".equals(s)){
            return null;
        }
        return getCorrentCell(s);
    }
    public String getCorrentCell(String s){
        if(s!=null){
            s=s.trim();
        }
        if(s==null||"".equals(s)){
            return null;
        }
        Pattern p=Pattern.compile(".*\\d+.*");
        Matcher m=p.matcher(s);
        if(!m.matches()){
            return null;
        }
        Pattern pattern=Pattern.compile("\\d+");
        Matcher matcher=pattern.matcher(s);
        String isNum=null;
        while (matcher.find()){
            isNum=matcher.group();
            if(isNum.length()==11&&phoneNum(isNum)==true){
                s=isNum;
                break;
            }  else{
                s=null;
            }
        }
        return s;
    }
    /**
     * 查询符合的手机号码
     *
     */
    public  boolean phoneNum(String num) {
//        将给定的手机电话正则表达式编译到模式中
        Pattern pattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(18[0,5-9]))\\d{8}");
//        创建匹配给定输入与此模式的匹配器
        Matcher matcher = pattern.matcher(num);
//       查找字符串中是否有符合的字符串
        while (matcher.find()) {
//            查询到符合手机号码规则即输出
            System.out.println("查找到手机号：" + matcher.group());
        }
        return matcher.matches();
    }
    /**
     * 查询符合固话的号码
     *
     */
    public static  void telpeNum(String telpe){
//        将给定的固话正则表达式编译到模式中
        Pattern pattern = Pattern.compile("(0\\d{2}-\\d{8}(-\\d{1,4})?)|(0\\d{3}-\\d{7,8}(-\\d{1,4})?)");
//        创建匹配给定输入匹配器
        Matcher matcher = pattern.matcher(telpe);
//        查找字符串中是否有符合固话规则的字符串
        while(matcher.find()){
//            如有合适固话规则输出即可
            System.out.println("查找到固话："+matcher.group());
        }

    }


}

