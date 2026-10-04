package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 单 Job 优化版：合并小文件，并在 Mapper 内按时间预聚合。 */
public class OptimizedSingleJobFactorDriver extends Configured implements Tool {

    private static final long DEFAULT_MAX_SPLIT_MB = 64L;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: OptimizedSingleJobFactorDriver "
                    + "<one-day-input> <output> [max-split-mb]");
            return 2;
        }

        long maxSplitMb = args.length == 3
                ? Long.parseLong(args[2])
                : DEFAULT_MAX_SPLIT_MB;
        if (maxSplitMb <= 0 || maxSplitMb > 2048) {
            throw new IllegalArgumentException("max-split-mb must be in [1, 2048]");
        }

        Configuration conf = getConf();
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");
        conf.setInt("mapreduce.task.io.sort.mb", 128);
        conf.setFloat("mapreduce.map.sort.spill.percent", 0.90f);
        conf.setInt("mapreduce.reduce.shuffle.parallelcopies", 10);

        Job job = Job.getInstance(conf,
                "Optimized SingleJob - Combined Files and InMapper Aggregate");
        job.setJarByClass(OptimizedSingleJobFactorDriver.class);

        job.setInputFormatClass(CombinedStockFileInputFormat.class);
        FileInputFormat.setInputDirRecursive(job, true);
        CombineFileInputFormat.setMaxInputSplitSize(job, maxSplitMb * 1024L * 1024L);

        job.setMapperClass(OptimizedSingleJobFactorMapper.class);
        // Mapper 已做预聚合；Combiner 保留为安全的二次本地合并。
        job.setCombinerClass(SingleJobFactorCombiner.class);
        job.setReducerClass(SingleJobFactorReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FactorVectorWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("Optimized input max split size: " + maxSplitMb + " MiB");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new Configuration(), new OptimizedSingleJobFactorDriver(), args);
        System.exit(exitCode);
    }
}
