package com.whc.odpsV3.UDF;

import com.aliyun.odps.udf.UDF;
import com.whc.tools.Tool;

public class Check_Sjhm extends UDF {
    public String evaluate(String lxdh) {
        String lxdh_format = "";
        if (lxdh != null) {
            if (Tool.verifyIsSjhm(lxdh)) {
                //通过基本验证
                if (lxdh.contains("+86")) {
                    lxdh_format = lxdh.replace("+86", "").replace("-", "").trim();
                    return lxdh_format;
                } else if (lxdh.trim().contains(" ")) {
                    lxdh_format = lxdh.trim().split(" ")[0];
                    return lxdh_format;
                } else if ((lxdh.trim().contains("-"))) {
                    return Tool.getSjhm(lxdh);
                } else {
                    lxdh_format = lxdh.trim();
                }
            }
        }
        return lxdh_format;
    }

    public static void main(String[] args) {
        Check_Sjhm check_sjhm = new Check_Sjhm();
        System.out.println(Tool.getSjhm(check_sjhm.evaluate("183-5839-2048")));
    }

}
