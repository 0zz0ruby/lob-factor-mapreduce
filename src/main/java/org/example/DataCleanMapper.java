package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * DataCleanMapper
 *
 * 将原始 Level-10 快照数据，清洗并整理为后续因子计算可直接输入。
 *
 * Input：
 * - key：输入偏移量（LongWritable）。
 * - value：原始 Level-10 行情快照。
 *   行内包含 tradingDay、tradeTime、code、bp/ap/bv/av 等字段。
 *
 * Output：
 * - outKey（Text）：分组键，采用 "tradingDay,code"。
 *   目的：同一天同一股票的所有快照进入同一 Reducer，便于排序与跨时刻因子计算。
 * - outValue（StockBean）：结构化快照对象。
 *   保存当前时刻的 tradeTime、最优价量与多档盘口等因子所需字段。
 *
 * 思路：
 * 1）Mapper 不做跨时间计算。只做清洗与结构化。
 * 2）尽早过滤不足的行。减少 Reduce 压力。保证下游因子不被异常值污染。
 */


public class DataCleanMapper extends Mapper<LongWritable, Text, Text, StockBean> {

    // 复用对象
    private final Text outKey = new Text();
    // 拼 key 用 StringBuilder，避免频繁字符串拼接。
    private final StringBuilder sb = new StringBuilder(64);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // MapReduce 输入可能出现空行/异常行，跳过
        if (value == null || value.getLength() == 0) return;

        String line = value.toString();
        if (line.isEmpty()) return;

        // 跳过表头行
        if (line.startsWith("tradingDay")) return;

        // 将一行 CSV 解析为stockbean对象。
        StockBean bean = StockBean.parse(line);
        if (bean == null) {
            context.getCounter("QC", "PARSE_FAIL").increment(1);
            return;
        }

        // 最优买卖价必须为正
        // top1 价格缺失或为 0，会导致价差/中间价/变动类因子整体失真。
        long[] bp = bean.getBp();
        long[] ap = bean.getAp();
        if (bp[0] <= 0 || ap[0] <= 0) {
            context.getCounter("QC", "BAD_PRICE_TOP1").increment(1);
            return;
        }


        // 取 tradingDay：从原始行拿首列，成本低且稳定。
        int p = line.indexOf(',');
        if (p <= 0) return;
        String tradingDay = line.substring(0, p);

        // 输出 key = tradingDay,code：同一股票同一天的数据聚到一起，Reducer 内部再按 tradeTime 排序。
        sb.setLength(0);
        sb.append(tradingDay).append(',').append(bean.getCode());
        outKey.set(sb.toString());

        // value 直接输出完整快照对象。
        context.write(outKey, bean);
    }
}
