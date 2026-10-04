package org.example;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 优化单 Job Mapper。
 *
 * 一个 Map task 会处理多只股票的完整文件。每个文件内部流式计算相邻快照因子，
 * task 内再按 tradeTime 累加，最后只为每个时刻输出一条 sum/count 统计量。
 */
public class OptimizedSingleJobFactorMapper
        extends Mapper<Text, NullWritable, LongWritable, FactorVectorWritable> {

    private static final long MARKET_OPEN = 93000L;
    private static final long MARKET_CLOSE = 150000L;
    private static final int DIM = 20;
    private static final int MAX_NORMALIZED_TIME = 235959;

    // HHMMSS 可以直接作为紧凑数组下标，避免每行创建 Long/Integer 包装对象。
    private final FactorVectorWritable[] aggregates =
            new FactorVectorWritable[MAX_NORMALIZED_TIME + 1];
    private final LongWritable outputKey = new LongWritable();
    private final double[] factors = new double[DIM];

    @Override
    protected void map(Text filePath, NullWritable ignored, Context context)
            throws IOException, InterruptedException {

        Path path = new Path(filePath.toString());
        FileSystem fs = path.getFileSystem(context.getConfiguration());

        FastStockSnapshot current = new FastStockSnapshot();
        FastStockSnapshot previous = new FastStockSnapshot();
        boolean hasPrevious = false;
        long previousTradeTime = Long.MIN_VALUE;
        long lineNumber = 0L;

        try (FSDataInputStream input = fs.open(path);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(input, StandardCharsets.UTF_8), 64 * 1024)) {

            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isEmpty() || line.startsWith("tradingDay")) continue;

                if (!current.parse(line)) {
                    context.getCounter("QC", "PARSE_FAIL").increment(1);
                    continue;
                }

                if (current.bp[0] <= 0 || current.ap[0] <= 0) {
                    context.getCounter("QC", "BAD_PRICE_TOP1").increment(1);
                    continue;
                }

                long tradeTime = normalizeTradeTime(current.tradeTime);
                if (tradeTime < 0 || tradeTime > MAX_NORMALIZED_TIME) {
                    context.getCounter("QC", "BAD_TRADE_TIME").increment(1);
                    continue;
                }

                if (hasPrevious && tradeTime < previousTradeTime) {
                    throw new IOException("snapshot.csv is not ordered by tradeTime: "
                            + path + " line " + lineNumber + ", " + tradeTime
                            + " follows " + previousTradeTime);
                }

                if (hasPrevious
                        && tradeTime >= MARKET_OPEN
                        && tradeTime <= MARKET_CLOSE) {
                    FactorMath.calculate(current, previous, factors);
                    addToAggregate((int) tradeTime, factors);
                }

                FastStockSnapshot tmp = previous;
                previous = current;
                current = tmp;
                hasPrevious = true;
                previousTradeTime = tradeTime;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int tradeTime = 0; tradeTime < aggregates.length; tradeTime++) {
            FactorVectorWritable aggregate = aggregates[tradeTime];
            if (aggregate == null) continue;
            outputKey.set(tradeTime);
            context.write(outputKey, aggregate);
        }
    }

    private void addToAggregate(int tradeTime, double[] values) {
        FactorVectorWritable aggregate = aggregates[tradeTime];
        if (aggregate == null) {
            aggregate = new FactorVectorWritable();
            aggregates[tradeTime] = aggregate;
        }

        double[] sums = aggregate.getSums();
        long[] counts = aggregate.getCounts();
        for (int i = 0; i < DIM; i++) {
            double value = values[i];
            if (Double.isFinite(value)) {
                sums[i] += value;
                counts[i]++;
            }
        }
    }

    private static long normalizeTradeTime(long tradeTime) {
        while (tradeTime > MAX_NORMALIZED_TIME) tradeTime /= 10L;
        return tradeTime;
    }
}
