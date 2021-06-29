package com.chinaoly.udtf.reduce;


import java.util.ArrayList;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class ReduceTaj extends GenericUDTF{
    public void close()
            throws HiveException
    {
    }
    public static void main(String[] args) throws HiveException   {
    	ReduceTaj t1 = new ReduceTaj();
    	Object[]  obj = {"152223196004307710::韩宝山::其他行政案件::2012年07月28日11时许，八一牧场二队居民韩宝山驾车与同队居民张晓龙相遇，张晓龙以韩宝山驾车刮着其胳膊为由，与韩宝山发生争吵后张晓龙先动手与韩宝山进行互殴。::内蒙古自治区扎赉特旗八一农场2委一区::20120728111002::嫌疑人::殴打他人##152223198801227739::张晓龙::其他行政案件::2012年07月28日11时许，八一牧场二队居民韩宝山驾车与同队居民张晓龙相遇，张晓龙以韩宝山驾车刮着其胳膊为由，与韩宝山发生争吵后张晓龙先动手与韩宝山进行互殴。::内蒙古自治区扎赉特旗八一农场2委一区::20120728111002::嫌疑人::殴打他人","J1522996612080900001"};
    	t1.process(obj);
    } 
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException
    {
        ArrayList fieldNames = new ArrayList();
        ArrayList fieldOIs = new ArrayList();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col3");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col4");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col5");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col6");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col7");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col8");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col9");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col10");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col11");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);




        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    public void process(Object[] args) throws HiveException
    {


        String jsondata = args[0].toString();
        String ajbh = args[1] == null? null: args[1].toString();
        String[] jsonArr = jsondata.split("##",-1);


        for (int i = 0; i < jsonArr.length; i++) {
            String[] nodeA = jsonArr[i].split("::",-1);
            for (int j = i + 1; j < jsonArr.length; j++) {
                String[] nodeB = jsonArr[j].split("::",-1);
                if (nodeA[0].equals(nodeB[0])) {
                    continue;
                }
               // System.out.println("..........nodeA"+nodeA.length);
               // System.out.println("..........nodeB"+nodeB.length);


                if (nodeA[0].compareTo(nodeB[0]) > 0) {
    
                    Object[] objects = {nodeB[0], nodeB[1], nodeA[0], nodeA[1], nodeB[2], nodeB[3], nodeB[4], nodeB[5], nodeB[6], nodeB[7], ajbh};
                    forward(objects);
                } else {
                    Object[] objects = {nodeA[0], nodeA[1], nodeB[0], nodeB[1], nodeA[2], nodeA[3], nodeA[4], nodeA[5], nodeA[6], nodeA[7], ajbh};
                    forward(objects);
                }
            }
        }
    }
}




