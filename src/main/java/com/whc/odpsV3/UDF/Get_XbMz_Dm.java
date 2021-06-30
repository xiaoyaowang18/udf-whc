package com.whc.odpsV3.UDF;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

//把一些不规则的名称转换为部标的代码字段
public class Get_XbMz_Dm extends UDF {
    public Get_XbMz_Dm() {
    }
    private static HashMap<String, String> map = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
            this.put("男", "1");
            this.put("男性", "1");
            this.put("女", "2");
            this.put("女性", "2");
            this.put("未知的性别", "0");
            this.put("未说明性别","9");
            this.put("汉", "01");
            this.put("蒙古", "02");
            this.put("回", "03");
            this.put("藏", "04");
            this.put("维吾尔", "05");
            this.put("苗", "06");
            this.put("彝", "07");
            this.put("壮", "08");
            this.put("布依", "09");
            this.put("朝鲜", "10");
            this.put("满", "11");
            this.put("侗", "12");
            this.put("瑶", "13");
            this.put("白", "14");
            this.put("土家", "15");
            this.put("哈尼", "16");
            this.put("哈萨克", "17");
            this.put("傣", "18");
            this.put("黎", "19");
            this.put("傈僳", "20");
            this.put("佤", "21");
            this.put("畲", "22");
            this.put("高山", "23");
            this.put("拉祜", "24");
            this.put("水", "25");
            this.put("东乡", "26");
            this.put("纳西", "27");
            this.put("景颇", "28");
            this.put("柯尔克孜", "29");
            this.put("土", "30");
            this.put("达斡尔", "31");
            this.put("仫佬", "32");
            this.put("羌", "33");
            this.put("布朗", "34");
            this.put("撒拉", "35");
            this.put("毛南", "36");
            this.put("仡佬", "37");
            this.put("锡伯", "38");
            this.put("阿昌", "39");
            this.put("普米", "40");
            this.put("塔吉克", "41");
            this.put("怒", "42");
            this.put("乌孜别克", "43");
            this.put("俄罗斯", "44");
            this.put("鄂温克", "45");
            this.put("德昂", "46");
            this.put("保安", "47");
            this.put("裕固", "48");
            this.put("京", "49");
            this.put("塔塔尔", "50");
            this.put("独龙", "51");
            this.put("鄂伦春", "52");
            this.put("赫哲", "53");
            this.put("门巴", "54");
            this.put("珞巴", "55");
            this.put("基诺", "56");
            this.put("穿青人", "59");
            this.put("亻革家人", "60");
            this.put("其他", "97");
            this.put("外国血统中国籍", "98");
            this.put("不详", "99");
            this.put("汉族", "01");
            this.put("蒙古族", "02");
            this.put("回族", "03");
            this.put("藏族", "04");
            this.put("维吾尔族", "05");
            this.put("苗族", "06");
            this.put("彝族", "07");
            this.put("壮族", "08");
            this.put("布依族", "09");
            this.put("朝鲜族", "10");
            this.put("满族", "11");
            this.put("侗族", "12");
            this.put("瑶族", "13");
            this.put("白族", "14");
            this.put("土家族", "15");
            this.put("哈尼族", "16");
            this.put("哈萨克族", "17");
            this.put("傣族", "18");
            this.put("黎族", "19");
            this.put("傈僳族", "20");
            this.put("佤族", "21");
            this.put("畲族", "22");
            this.put("高山族", "23");
            this.put("拉祜族", "24");
            this.put("水族", "25");
            this.put("东乡族", "26");
            this.put("纳西族", "27");
            this.put("景颇族", "28");
            this.put("柯尔克孜族", "29");
            this.put("土族", "30");
            this.put("达斡尔族", "31");
            this.put("仫佬族", "32");
            this.put("羌族", "33");
            this.put("布朗族", "34");
            this.put("撒拉族", "35");
            this.put("毛南族", "36");
            this.put("仡佬族", "37");
            this.put("锡伯族", "38");
            this.put("阿昌族", "39");
            this.put("普米族", "40");
            this.put("塔吉克族", "41");
            this.put("怒族", "42");
            this.put("乌孜别克族", "43");
            this.put("俄罗斯族", "44");
            this.put("鄂温克族", "45");
            this.put("德昂族", "46");
            this.put("保安族", "47");
            this.put("裕固族", "48");
            this.put("京族", "49");
            this.put("塔塔尔族", "50");
            this.put("独龙族", "51");
            this.put("鄂伦春族", "52");
            this.put("赫哲族", "53");
            this.put("门巴族", "54");
            this.put("珞巴族", "55");
            this.put("基诺族", "56");
            this.put("穿青人族", "59");
            this.put("亻革家人族", "60");
            this.put("其他族", "97");
            this.put("外国血统中国籍族", "98");
            this.put("不详族", "99");
        }
    };



    public static void main(String[] args) {
        Get_XbMz_Dm cl = new Get_XbMz_Dm();
        System.out.println(cl.evaluate("男性"));
    }

    public String evaluate(String s) {
        return (String)map.get(s);
    }
}
