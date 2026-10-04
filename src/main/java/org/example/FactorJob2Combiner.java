package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FactorJob2Combiner
 *
 * - 这是 Job2 的 Combiner本地预聚合，减少网络 Shuffle 的数据量。
 *
 * Input：
 * - key：时间维度的分组键（LongWritable），tradeTime。
 * - values：同一 key 下，多条 FactorVectorWritable。
 *
 * 输出（Output）：
 * - key：保持不变，tradeTime。
 * - value：聚合后的 FactorVectorWritable（同一 key 的向量逐元素求和）。
 *
 * 思路：
 * 1）因子最终要按时间做横截面平均。
 *    上游会输出：sum(alpha_i) 与 count 等可加的统计量。
 * 2）这里处理的是各因子分子和、样本数
 *    使用 Combiner 做局部累加。
 * 3）Combiner 的目的只是“提前加一部分”。
 *    结果仍然等价于全部交给 Reducer 再加。
 *
 */
public class FactorJob2Combiner extends Reducer<LongWritable, FactorVectorWritable, LongWritable, FactorVectorWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<FactorVectorWritable> values, Context context)
            throws IOException, InterruptedException {

        FactorVectorWritable agg = new FactorVectorWritable();
        for (FactorVectorWritable v : values) agg.add(v);
        context.write(key, agg);
    }
}
