package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FactorJob2Reducer
 *
 * - 按 tradeTime 做横截面聚合，输出每个时刻 20 个因子的平均值
 * - 支持 reducer > 1 的并行执行
 * - 表头仅由 reducer-0 输出一次
 */
public class FactorJob2Reducer
        extends Reducer<LongWritable, FactorVectorWritable,
        NullWritable, Text> {

    private static final int DIM = 20;

    /** 当前 reducer 是否负责输出表头 */
    private boolean writeHeader = false;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        // reducer 的全局编号（0,1,2,...）
        int reducerId = context.getTaskAttemptID()
                .getTaskID()
                .getId();

        // 只允许 reducer-0 写表头
        writeHeader = (reducerId == 0);
    }

    @Override
    protected void reduce(LongWritable key,
                          Iterable<FactorVectorWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        // 🔎 计数器：统计 reduce 调用次数（调试/验算用）
        // "VER"：Counter Group（分组名）
        //"JOB2_V5"：Counter 名字
        context.getCounter("VER", "JOB2_V5").increment(1);

        // reducer-0 仅在第一次 reduce 调用时输出表头
        if (writeHeader) {
            StringBuilder header = new StringBuilder("tradeTime");
            for (int i = 1; i <= DIM; i++) {
                header.append(",alpha_").append(i);
            }
            context.write(NullWritable.get(), new Text(header.toString()));
            writeHeader = false; // 确保只写一次
        }

        double[] sums = new double[DIM];
        long[] counts = new long[DIM];

        // 横截面累计
        for (FactorVectorWritable v : values) {
            double[] s = v.getSums();
            long[] c = v.getCounts();
            for (int i = 0; i < DIM; i++) {
                sums[i] += s[i];
                counts[i] += c[i];
            }
        }

        // 输出一行 CSV
        StringBuilder line = new StringBuilder();
        line.append(String.format("%06d", key.get()));

        for (int i = 0; i < DIM; i++) {
            double avg = (counts[i] == 0L) ? 0.0 : (sums[i] / counts[i]);
            line.append(",").append(avg);
        }

        context.write(NullWritable.get(), new Text(line.toString()));
    }
}
