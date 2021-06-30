package com.whc.odpsV3.UDAF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve("double->double")
public class AggrAvg extends Aggregator {

    private static class AvgBuffer implements Writable {
        private double sum = 0;
        private long count = 0;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
        }
    }

    private DoubleWritable ret = new DoubleWritable();

    /**
     * 创建初始返回结果的值。
     *
     * @return
     */
    @Override
    public Writable newBuffer() {
        return new AvgBuffer();
    }

    /**
     * @param buffer
     * @param args   writable是指一个阶段性的汇总数据，在不同的Map任务中，group by后得出的数据（可理解为一个集合），每行执行一次。
     * @throws UDFException
     */
    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
        DoubleWritable arg = (DoubleWritable) args[0];
        AvgBuffer buf = (AvgBuffer) buffer;
        if (arg != null) {
            buf.count += 1;
            buf.sum += arg.get();
        }
    }

    /**
     * 返回数据
     *
     * @param buffer
     * @return
     * @throws UDFException
     */
    @Override
    public Writable terminate(Writable buffer) throws UDFException {
        AvgBuffer buf = (AvgBuffer) buffer;
        if (buf.count == 0) {
            ret.set(0);
        } else {
            ret.set(buf.sum / buf.count);
        }
        return ret;
    }

    /**
     * 将不同的Map直接结算的结果进行汇总。
     *
     * @param buffer
     * @param partial
     * @throws UDFException
     */
    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        AvgBuffer buf = (AvgBuffer) buffer;
        AvgBuffer p = (AvgBuffer) partial;
        buf.sum += p.sum;
        buf.count += p.count;
    }
}