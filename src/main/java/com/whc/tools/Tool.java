package com.whc.tools;

import org.apache.commons.lang.StringUtils;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tool {
    private static String[] areaCodeArr = {"11", "12", "13", "14", "15", "21",
            "22", "23", "31", "32", "33", "34", "35", "36", "37", "41", "42",
            "43", "44", "45", "46", "50", "51", "52", "53", "54", "61", "62",
            "63", "64", "65", "71", "81", "82", "91"};

    private static int[] wi = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8,
            4, 2};

    private static char[] verifycode = {'1', '0', 'X', '9', '8', '7', '6', '5',
            '4', '3', '2'};
    private static int nowYear = Calendar.getInstance().get(1);
    private static final double EARTH_RADIUS = 6378137.0D;

    public static boolean verifyIsSfzh(String sfzh) {
        String pattern = "(^[1-9]\\d{16}[0-9xX]$)|(^[1-9]\\d{14}$)";
        return Pattern.compile(pattern).matcher(sfzh).matches();
    }

    public static boolean verifyIsCphm(String cphm) {
        String pattern = "(^[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领A-Z]{1}[A-Z]{1}[A-Z0-9]{4}[A-Z0-9挂学警港澳]{1}$)";
        return Pattern.compile(pattern).matcher(cphm).matches();
    }

    public static boolean verifyIsCphm_yn(String cphm) {
        String pattern = "(^[A-Z]{1}[A-Z0-9]{4}[A-Z0-9挂学警港澳]{1}$)";
        return Pattern.compile(pattern).matcher(cphm).matches();
    }

    public static boolean verifyIsSjhm(String dhhm) {
        String pattern = "(.*(1[3458]\\d)-?(\\d{4})-?(\\d{4}).*)";
        return Pattern.compile(pattern).matcher(dhhm).matches();
    }

    public static String getSjhm(String dhhm) {
        String pattern = ".*(1[3458]\\d)-?(\\d{4})-?(\\d{4}).*";
        Matcher matcher = Pattern.compile(pattern).matcher(dhhm);
        if (matcher.find()) {
            return matcher.group(1) + matcher.group(2) + matcher.group(3);
        }
        return null;
    }

    public static boolean verifyIsDhhm(String dhhm) {
        String pattern = ".*(0\\d{2,3})-?([2-9]\\d{6,7}).*";
        return Pattern.compile(pattern).matcher(dhhm).matches();
    }

    public static String getDhhm(String dhhm) {
        String pattern = ".*(0\\d{2,3})-?([2-9]\\d{6,7}).*";
        Matcher matcher = Pattern.compile(pattern).matcher(dhhm);
        if (matcher.find()) {
            return matcher.group(1) + matcher.group(2);
        }
        return null;
    }

    public static boolean verifyAreaCode(String sfzh) {
        String areaCode = sfzh.substring(0, 2);
        for (String code : areaCodeArr) {
            if (code.equals(areaCode)) {
                return true;
            }
        }
        return false;
    }

    public static boolean verifyBirthday(String date) {
        if (verifyDate(date)) {
            int year = Integer.parseInt(date.substring(0, 4));
            if ((year >= 1900) && (year <= nowYear)) {
                return true;
            }
        }
        return false;
    }

    public static boolean verifyDate(String date) {
        date = StringUtils.trimToEmpty(date);

        String pattern = "(^\\d{8}$|^\\d{10}$|^\\d{12}$|^\\d{14}$)";
        if (!(Pattern.compile(pattern).matcher(date).matches())) {
            return false;
        }

        int year = Integer.parseInt(date.substring(0, 4));
        if (year < 1000) {
            return false;
        }
        int month = Integer.parseInt(date.substring(4, 6));
        int day = Integer.parseInt(date.substring(6, 8));

        boolean isLeapYear = false;

        if (( year % 400 == 0)
                || (((int) year % 100 != 0) && ((int) year % 4 == 0))) {
            isLeapYear = true;
        }

        if (((int) month < 1) || ((int) month > 12)) {
            return false;
        }

        if (((int) month == 1) || ((int) month == 3)
                || ((int) month == 5) || ((int) month == 7)
                || ((int) month == 8) || ((int) month == 10)
                || ((int) month == 12)) {
            if (((int) day < 1) || ((int) day > 31))
                return false;
        } else if (((int) month == 4) || ((int) month == 6)
                || ((int) month == 9) || ((int) month == 11)) {
            if (((int) day < 1) || ((int) day > 30))
                return false;
        } else {
            if (isLeapYear) {
                if (((int) day < 1) || ((int) day > 29)) {
                    return false;
                }
            } else if (((int) day < 1) || ((int) day > 28)) {
                return false;
            }

        }

        if (date.length() >= 10) {
            int hour = Integer.parseInt(date.substring(8,
                    10));
            if ( hour > 23) {
                return false;
            }
        }

        if (date.length() >= 12) {
            int minute = Integer.parseInt(date.substring(
                    10, 12));
            if (minute > 59) {
                return false;
            }
        }

        if (date.length() == 14) {
            int second = Integer.parseInt(date.substring(
                    12, 14));
            if ( second > 59) {
                return false;
            }

        }

        return true;
    }

    public static String getTime(String date, int resultLength) {
        date = StringUtils.trimToEmpty(date).replaceAll("([^0-9])", " ").trim();
        String[] dateArr = date.split(" ");
        int arrLength = dateArr.length;

        if (arrLength == 1) {
            date = dateArr[0];
        } else if ((arrLength > 1) && (dateArr[0].length() == 4)) {
            date = dateArr[0];

            for (int i = 1; i < arrLength; ++i) {
                if (dateArr[i].length() == 1) {
                    date = date + '0' + dateArr[i];
                } else if (dateArr[i].length() == 2)
                    date = date + dateArr[i];
            }
        } else {
            date = "";
        }

        date = StringUtils.substring(date, 0, 14);

        if (verifyDate(date)) {
            int length = date.length();
            for (int i = resultLength; i > length; --i) {
                date = date + "0";
            }

            date = date.substring(0, resultLength);
            return date;
        }
        return null;
    }

    public static String getEighteenIDCard(String sfzh) {
        sfzh = StringUtils.trimToEmpty(sfzh);
        if (sfzh.length() == 15) {
            StringBuffer sb = new StringBuffer();
            sb.append(sfzh.substring(0, 6)).append("19")
                    .append(sfzh.substring(6))
                    .append(getVerifycode(sb.toString()));
            return sb.toString();
        }
        if (sfzh.contains("x")) {
            sfzh = StringUtils.replace(sfzh, "x", "X");
        }
        if (sfzh.length() != 18) {
            sfzh = null;
        }
        return sfzh;
    }

    public static char getVerifycode(String sfzh) {
        char[] ai = sfzh.toCharArray();
        int s = 0;
        int y = 0;
        for (int i = 0; i < wi.length; ++i) {
            s += (ai[i] - '0') * wi[i];
        }
        y = s % 11;
        return verifycode[y];
    }

    public static boolean verifyMOD(String sfzh) {
        char verify = sfzh.charAt(17);

        return (verify == getVerifycode(sfzh));
    }

    private static double rad(double d) {
        return (d * 3.141592653589793D / 180.0D);
    }

    public static Double getDistance(double lng1, double lat1, double lng2,
                                     double lat2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2.0D * Math
                .asin(Math.sqrt(Math.pow(Math.sin(a / 2.0D), 2.0D)
                        + Math.cos(radLat1) * Math.cos(radLat2)
                        * Math.pow(Math.sin(b / 2.0D), 2.0D)));

        s *= 6378137.0D;
        s = Math.round(s * 10000.0D) / 10000L;
        return s;
    }

    public static double sigmoid(double x, double t) {
        return (1.0D / (1.0D + Math.exp(-1.0D * t * x)));
    }

    public static boolean verifyDBC(String str) {
        if (str != null) {
            for (char c : str.toCharArray()) {
                if (((c > 65280) && (c < 65375)) || (c == 12288)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean verifyTrim(String str) {
        return ((str == null) || (str.length() == str.trim().length()));
    }

    public static boolean verifySpecialChar(String str) {
        if (str != null) {
            String pattern = "[^\\r\\n]*[^\\\\\\r\\n]|$";
            return Pattern.compile(pattern).matcher(str).matches();
        }
        return true;
    }

    public static boolean verifyChinese(String str) {
        if (str != null) {
            String pattern = "^[一-龥]*$";
            return Pattern.compile(pattern).matcher(str).matches();
        }
        return true;
    }

    public static boolean verifyNumberAndLetter(String str) {
        if (str != null) {
            String pattern = "^[0-9a-zA-Z]*$";
            return Pattern.compile(pattern).matcher(str).matches();
        }
        return true;
    }

    public static String strAndCompute(String str1, String str2) {
        if ((str1 == null) && (str2 != null))
            return str2;
        if ((str1 != null) && (str2 == null))
            return str1;
        if ((str1 != null) && (str2 != null)) {
            String[] strArr1 = str1.split(" ");
            String[] strArr2 = str2.split(" ");
            String result = "";
            int length = strArr1.length;
            if (length != strArr2.length) {
                return "";
            }
            for (int i = 0; i < length; ++i) {
                result = result
                        + " "
                        + (Integer.parseInt(strArr1[i]) & Integer
                        .parseInt(strArr2[i]));
            }
            return result.trim();
        }
        return "";
    }

    public static boolean verifySfzh(String sfzh) {
        sfzh = StringUtils.trimToEmpty(sfzh);

        if (verifyIsSfzh(sfzh)) {
            sfzh = getEighteenIDCard(sfzh);

            if ((sfzh != null) && (verifyAreaCode(sfzh))
                    && (verifyBirthday(StringUtils.substring(sfzh, 6, 14)))
                    && (verifyMOD(sfzh))) {
                return true;
            }
        }
        return false;
    }

    public static String quickSort(String str) {
        str = StringUtils.trimToEmpty(str);
        char[] charArr = str.toCharArray();
        sort(charArr, 0, charArr.length - 1);
        return String.valueOf(charArr);
    }

    public static void sort(char[] a, int low, int high) {
        int start = low;
        int end = high;
        char key = a[low];

        while (end > start) {
            while ((end > start) && (a[end] >= key)) {
                --end;
            }
            if (a[end] <= key) {
                char temp = a[end];
                a[end] = a[start];
                a[start] = temp;
            }
            while ((end > start) && (a[start] <= key)) {
                ++start;
            }
            if (a[start] >= key)
                ;
            char temp = a[start];
            a[start] = a[end];
            a[end] = temp;
        }

        if (start > low) {
            sort(a, low, start - 1);
        }
        if (end < high)
            sort(a, end + 1, high);
    }


    public static void main(String[] args) {
    }
}