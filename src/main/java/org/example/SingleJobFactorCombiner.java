package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** Map 端只做 sum/count 累加，满足结合律和交换律。 */
public class SingleJobFactorCombiner extends Reducer<LongWritable, FactorVectorWritable,
        LongWritable, FactorVectorWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<FactorVectorWritable> values, Context context)
            throws IOException, InterruptedException {
        FactorVectorWritable aggregate = new FactorVectorWritable();
        for (FactorVectorWritable value : values) aggregate.add(value);
        context.write(key, aggregate);
    }
}
