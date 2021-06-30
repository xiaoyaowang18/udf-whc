package com.whc.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

//人员主题库中使用，取多个来源中的最优值。
@SuppressWarnings("deprecation")
public class GenericUdafOptimal extends UDAF {
    public static class OptimalUDAFEvaluator implements UDAFEvaluator {
        //最终结果
        private ParaBuffer result;

        private static class ParaBuffer {
            //要筛选的字段
            private String para;
            //更新时间
            private Long gxsj;
            //表权重
            private Integer bqz;

            @Override
            public String toString() {
                return para;
            }
        }

        //负责初始化计算函数并设置它的内部状态，result是存放最终结果的
        @Override
        public void init() {
            result = null;
        }

        //每次对一个新值进行聚集计算都会调用iterate方法
        public boolean iterate(String para, String gxsjStr, String bqzStr) {
            if (para == null)
                return true;
            long gxsj = 0;
            int bqz = Integer.MAX_VALUE;
            if (gxsjStr != null && gxsjStr.length() < 15)
                gxsj = Long.parseLong(gxsjStr);
            if (bqzStr != null) bqz = Integer.parseInt(bqzStr);
            if (result == null) {
                result = new ParaBuffer();
                result.para = para;
                result.gxsj = gxsj;
                result.bqz = bqz;
            }
            //gxsj存在的情况
            if (gxsj != 0) {
                //比较，更新聚合buffer
                if (bqz < result.bqz || (bqz == result.bqz && gxsj > result.gxsj)) {
                    result.para = para;
                    result.gxsj = gxsj;
                    result.bqz = bqz;
                }
            } else {  //gxsj不存在的情况
                if (bqz <= result.bqz) {
                    result.para = para;
                    result.gxsj = 0l;
                    result.bqz = bqz;
                }
            }
            return true;
        }

        //Hive需要部分聚集结果的时候会调用该方法
        //会返回一个封装了聚集计算当前状态的对象
        public ParaBuffer terminatePartial() {
            return result;
        }

        //合并两个部分聚集值会调用这个方法
        public boolean merge(ParaBuffer other) {
            if (other == null) return false;
            if (result == null) {
                result = new ParaBuffer();
                result.bqz = other.bqz;
                result.gxsj = other.gxsj;
                result.para = other.para;
                return true;
            }

            //gxsj存在的情况
            if (other.gxsj != 0) {
                //比较，更新聚合buffer
                if (other.bqz < result.bqz || (other.bqz == result.bqz && other.gxsj > result.gxsj)) {
                    result.para = other.para;
                    result.gxsj = other.gxsj;
                    result.bqz = other.bqz;
                }
            } else {  //gxsj不存在的情况
                if (other.bqz <= result.bqz) {
                    result.para = other.para;
                    result.gxsj = 0l;
                    result.bqz = other.bqz;
                }
            }
            return true;
        }

        //Hive需要最终聚集结果时候会调用该方法
        public String terminate() {
            if (result == null) return null;
            return result.toString();
        }
    }

}