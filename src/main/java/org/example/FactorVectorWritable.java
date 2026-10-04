package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FactorVectorWritable
 *
 * - 用“可加统计量”表示 20 维因子向量，便于 Combiner和Reducer 做聚合。
 *
 * - Reducer 要输出的是“按时间的横截面平均值”。
 * - 平均值需要 sum / count。
 * - 但是某些股票某些时刻的因子可能无效（NaN/Inf），不能计入平均，因此不是直接存 20 个因子值
 * 因此我们为每一维独立地维护两个量：
 * - sums[i]：第 i 个因子的分子累积和。
 * - counts[i]：第 i 个因子的有效样本数。
 */
public class FactorVectorWritable implements Writable {

    private static final int DIM = 20;

    //各因子累加
    private final double[] sums = new double[DIM];
    //有效样本数量
    private final long[] counts = new long[DIM];

    public FactorVectorWritable() {}

    /**
     * 将“单只股票在某时刻的 20 个因子值”转为可聚合表示。
     *
     * f[i]正常，则 sums[i]=f[i], counts[i]=1。
     * f[i]=NaN/Inf（无效值），则 sums[i]=0, counts[i]=0，不计入平均。
     *
     * - 把“过滤无效值”放到 Mapper。
     * - Reducer 只做加法与除法，逻辑简单更稳定。
     */

    public static FactorVectorWritable fromFactors(double[] f) {
        FactorVectorWritable v = new FactorVectorWritable();
        for (int i = 0; i < DIM; i++) {
            double x = f[i];
            if (Double.isFinite(x)) {
                v.sums[i] = x;
                v.counts[i] = 1L;
            } else {
                v.sums[i] = 0.0;
                v.counts[i] = 0L;
            }
        }
        return v;
    }

    public void add(FactorVectorWritable other) {
        for (int i = 0; i < DIM; i++) {
            this.sums[i] += other.sums[i];
            this.counts[i] += other.counts[i];
        }
    }

    // 暴露内部数组引用以减少拷贝。调用时只读，不修改数组内容。
    public double[] getSums() { return sums; }
    public long[] getCounts() { return counts; }

    /**
     * Writable 序列化。
     *
     * 写入顺序必须与 readFields 完全一致。
     * 先写 20 个 double，再写 20 个 long。 顺序固定，解析成本低。
     * 大量中间结果在网络中传输适用。
     */
    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < DIM; i++) out.writeDouble(sums[i]);
        for (int i = 0; i < DIM; i++) out.writeLong(counts[i]);
    }

    //读取时与write一致，先sums再counts
    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < DIM; i++) sums[i] = in.readDouble();
        for (int i = 0; i < DIM; i++) counts[i] = in.readLong();
    }
}
