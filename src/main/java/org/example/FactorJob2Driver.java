package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FactorJob2Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: FactorJob2Driver <input> <output>");
            return -1;
        }

        Configuration conf = getConf();

// 读取 Job1 输出的 key\tvalue
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPARATOR, "\t");

        /* ==
         * [TUNING] Shuffle 提速
         * == */
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        conf.setInt("mapreduce.reduce.shuffle.parallelcopies", 10);

        Job job = Job.getInstance(conf, "FactorJob2 - CrossSection Mean");

        job.setJarByClass(FactorJob2Driver.class);

        // 输出时 key 和 value 之间不插入分隔符（因为 reducer 输出 key 是 NullWritable）
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        // InputFormat：读取 Job1 的 "key\tvalue"
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // Mapper / Combiner / Reducer
        job.setMapperClass(FactorJob2Mapper.class);
        job.setCombinerClass(FactorJob2Combiner.class);
        job.setReducerClass(FactorJob2Reducer.class);

        // Map 输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FactorVectorWritable.class);

        //同job1 driver：开启 Mapper → Reducer 的“中间结果”压缩（只影响 shuffle / spill）
        conf.setBoolean("mapreduce.map.output.compress", true);
        //压缩算法：Snappy 速度快（省网络/磁盘 IO）
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");


        // Reduce 最终输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 单 reducer
        job.setNumReduceTasks(3);

        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FactorJob2Driver(), args);
        System.exit(exitCode);
    }
}
