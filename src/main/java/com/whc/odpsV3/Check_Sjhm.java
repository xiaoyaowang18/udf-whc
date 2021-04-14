package com.whc.odpsV3;

import com.aliyun.odps.udf.UDF;
import com.whc.tools.Tool;

public class Check_Sjhm extends UDF {
    public String evaluate(String lxdh) {
        String lxdh_format = "";
        if (lxdh != null) {
            if (Tool.verifyIsSjhm(lxdh)) {//通过基本验证
                if (lxdh.contains("+86")) {
                    lxdh_format = lxdh.replace("+86", "").replace("-", "").trim();
                    return lxdh_format;
                }
                if (lxdh.trim().contains(" ")) {
                    lxdh_format = lxdh.trim().split(" ")[0];
                    return lxdh_format;
                } else {
                    lxdh_format = lxdh.trim();
                }

            }
        }
        return lxdh_format;
    }
}
