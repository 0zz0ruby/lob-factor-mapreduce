package org.example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * CombineFileRecordReader 会为组合切片中的每个文件创建一次本类。
 * 每个文件只输出一条记录：key 是 HDFS 文件路径，value 为空。
 */
public class CombinedStockFileRecordReader
        extends RecordReader<Text, NullWritable> {

    private final Text filePath;
    private boolean emitted;

    /** 该构造器签名由 CombineFileRecordReader 通过反射调用。 */
    public CombinedStockFileRecordReader(
            CombineFileSplit split,
            TaskAttemptContext context,
            Integer index) {
        this.filePath = new Text(split.getPath(index).toString());
        this.emitted = false;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        // 所需的文件路径已在构造器中取得。
    }

    @Override
    public boolean nextKeyValue() {
        if (emitted) return false;
        emitted = true;
        return true;
    }

    @Override
    public Text getCurrentKey() {
        return filePath;
    }

    @Override
    public NullWritable getCurrentValue() {
        return NullWritable.get();
    }

    @Override
    public float getProgress() {
        return emitted ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // 本类不持有流；文件由 Mapper 流式打开并关闭。
    }
}
