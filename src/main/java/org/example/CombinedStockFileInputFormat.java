package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * 把多个完整的 snapshot.csv 合并到同一个 Map task 中。
 *
 * 每只股票的文件必须保持完整，不能把一个文件切成两段，否则相邻快照因子会在
 * 文件切分点丢失。因此这里显式返回 false，并由 CombinedStockFileRecordReader
 * 为每个完整文件产生一条“文件路径”记录。
 */
public class CombinedStockFileInputFormat
        extends CombineFileInputFormat<Text, NullWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split,
                context,
                CombinedStockFileRecordReader.class);
    }
}
