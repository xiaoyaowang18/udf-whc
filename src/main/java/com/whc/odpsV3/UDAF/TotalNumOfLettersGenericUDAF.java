package com.whc.odpsV3.UDAF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


@Description(name = "letters", value = "__FUNC__(expr) - return the total count chars of the column(返回该列中所有字符串的字符总数)")
public class TotalNumOfLettersGenericUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) { // 判断参数长度
            throw new UDFArgumentLengthException("Exactly one argument is expected, but " +
                    parameters.length + " was passed!");
        }

        ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

        if (objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) { // 是不是标准的java Object的primitive类型
            throw new UDFArgumentTypeException(0, "Argument type must be PRIMARY. but " +
                    objectInspector.getCategory().name() + " was passed!");
        }

        // 如果是标准的java Object的primitive类型，说明可以进行类型转换
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) objectInspector;

        // 如果是标准的java Object的primitive类型,判断是不是string类型，因为参数只接受string类型
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Argument type must be Strig, but " +
                    inputOI.getPrimitiveCategory().name() + " was passed!");
        }

        return new TotalNumOfLettersEvaluator();
    }

    public static class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputIO;
        ObjectInspector outputIO;
        PrimitiveObjectInspector IntegerIO;

        int total = 0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            /**
             * PARTIAL1: 这个是mapreduce的map阶段:从原始数据到部分数据聚合
             * 将会调用iterate()和terminatePartial()

             * PARTIAL2: 这个是mapreduce的map端的Combiner阶段，负责在map端合并map的数据::从部分数据聚合到部分数据聚合:
             * 将会调用merge() 和 terminatePartial()

             * FINAL: mapreduce的reduce阶段:从部分数据的聚合到完全聚合
             * 将会调用merge()和terminate()

             * COMPLETE: 如果出现了这个阶段，表示mapreduce只有map，没有reduce，所以map端就直接出结果了:从原始数据直接到完全聚合
             * 将会调用 iterate()和terminate()
             */

            //map阶段读取sql列，输入为String基础数据格式
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputIO = (PrimitiveObjectInspector) parameters[0];
            } else { //其余阶段，输入为Integer基础数据格式
                IntegerIO = (PrimitiveObjectInspector) parameters[0];
            }

            // 指定各个阶段输出数据格式都为Integer类型
            outputIO = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputIO;
        }

        /**
         * 存储当前字符总数的类
         */
        static class LetterSumAgg implements AggregationBuffer {
            int sum = 0;

            void add(int num) {
                sum += num;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LetterSumAgg result = new LetterSumAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            LetterSumAgg myAgg = new LetterSumAgg();
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                LetterSumAgg myAgg = (LetterSumAgg) agg;
                Object p = inputIO.getPrimitiveJavaObject(parameters[0]);
                myAgg.add(String.valueOf(p).length());
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myAgg = (LetterSumAgg) agg;
            total += myAgg.sum;
            return total;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                LetterSumAgg myAgg1 = (LetterSumAgg) agg;
                Integer partialSum = (Integer) IntegerIO.getPrimitiveJavaObject(partial);
                LetterSumAgg myAgg2 = new LetterSumAgg();
                myAgg2.add(partialSum);
                myAgg1.add(myAgg2.sum);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myAgg = (LetterSumAgg) agg;
            total = myAgg.sum;
            return myAgg.sum;
        }
    }

}