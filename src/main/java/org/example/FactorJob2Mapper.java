package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FactorJob2Mapper
 *
 * - Job2 的 Mapper，负责把“按股票/日期聚合后的因子向量”转换为“按时间聚合”的形式。
 * - 做的是 key 的更改，未做统计汇总。
 *
 * Input：
 * - key（Text）：job1输出的复合键字符串：tradingDay,tradeTime,code
 *
 *   但我们只需要第二段：tradeTime。
 * - value（Text）：20 个因子值的字符串表示，逗号分隔。
 *
 * Output：
 * - outKey（LongWritable）：规整后的时间键 t（hhmmss）。
 *   目的：同一时刻 t 的所有股票样本进入同一个 Reducer。
 * - outValue（FactorVectorWritable）：封装的 20 维因子向量。
 *   Reducer/Combiner 将对其做逐元素累加，最终再除以样本数得到平均值序列。
 *
 * 思路：
 * 1）最终目标是按 tradeTime 输出横截面平均因子值。
 *    所以需要把 key 变成“时间”。
 * 2）tradeTime  hhmmss。
 * 3）只保留连续竞价时段 [093000, 150000] 提前过滤可减轻reducer压力。
 */

public class FactorJob2Mapper extends Mapper<Text, Text, LongWritable, FactorVectorWritable> {

    // 复用 outKey，减少对象创建。
    private final LongWritable outKey = new LongWritable();
    private final double[] factors = new double[20];

    @Override
    protected void map(Text key, Text value, Context ctx) throws IOException, InterruptedException {

        // key 为复合字符串，取出中间的是 tradeTime。
        String ks = key.toString();
        int c1 = ks.indexOf(',');
        if (c1 < 0) return;
        int c2 = ks.indexOf(',', c1 + 1);
        // 至少需要两个逗号，才能取到中间段
        if (c2 < 0) return;

        long t;
        try {
            //readetime转Long
            t = Long.parseLong(ks.substring(c1 + 1, c2).trim());
        } catch (Exception e) {
            return;
        }

        while (t > 235959) t /= 10; // 规整到 hhmmss


        if (t < 93000 || t > 150000) return;

        // value 为逗号分隔的 20 个 double。
        String vs = value.toString();
        if (!parse20Doubles(vs, factors)) return;

        // 按时间重分组 key = t（hhmmss）
        // value = 20 维因子向量
        // 下游 Combiner/Reducer 将按时间聚合，最终得到该时刻的横截面平均因子值。
        outKey.set(t);
        ctx.write(outKey, FactorVectorWritable.fromFactors(factors));
    }

    /**
     * 将一行逗号分隔的字符串解析为 20 个 double。
     *
     * 实现选择手写扫描，而不是 split：
     * - split 会产生大量临时 String 对象，开销较大。
     * - 高频数据行数巨大，手写扫描更稳。
     */
    private static boolean parse20Doubles(String s, double[] out) {
        int n = s.length();
        int idx = 0;
        int start = 0;

        // 扫描到逗号或行尾就切片解析一个 token。
        for (int i = 0; i <= n; i++) {
            if (i == n || s.charAt(i) == ',') {
                if (idx >= 20) break;
                String token = s.substring(start, i).trim();
                if (token.isEmpty()) return false;
                try {
                    out[idx++] = Double.parseDouble(token); // "NaN" 也能解析，FactorVectorWritable会舍弃
                } catch (Exception e) {
                    return false;
                }
                start = i + 1;
            }
        }

        return idx >= 20;
    }
}
