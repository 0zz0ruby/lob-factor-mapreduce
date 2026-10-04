package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** 汇总同一时刻所有股票的因子，并输出按时间排序的横截面平均值。 */
public class SingleJobFactorReducer extends Reducer<LongWritable, FactorVectorWritable,
        NullWritable, Text> {

    private static final int DIM = 20;
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        StringBuilder header = new StringBuilder("tradeTime");
        for (int i = 1; i <= DIM; i++) header.append(",alpha_").append(i);
        outputValue.set(header.toString());
        context.write(NullWritable.get(), outputValue);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<FactorVectorWritable> values, Context context)
            throws IOException, InterruptedException {

        double[] sums = new double[DIM];
        long[] counts = new long[DIM];

        for (FactorVectorWritable value : values) {
            double[] partialSums = value.getSums();
            long[] partialCounts = value.getCounts();
            for (int i = 0; i < DIM; i++) {
                sums[i] += partialSums[i];
                counts[i] += partialCounts[i];
            }
        }

        StringBuilder line = new StringBuilder(384);
        line.append(String.format("%06d", key.get()));
        for (int i = 0; i < DIM; i++) {
            double average = counts[i] == 0L ? 0.0 : sums[i] / counts[i];
            line.append(',').append(average);
        }

        outputValue.set(line.toString());
        context.write(NullWritable.get(), outputValue);
    }
}
