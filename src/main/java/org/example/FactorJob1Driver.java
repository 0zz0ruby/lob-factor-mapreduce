package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 整体作用：
 * 1) Mapper: 使用 DataCleanMapper 读取原始行情数据，完成清洗/解析，并输出结构化的 StockBean。
 * 2) Reducer: 使用 FactorCalcReducer 对 Mapper 输出按 key 聚合，并输出文本结果（Text）。
 */
public class FactorJob1Driver {

    public static void main(String[] args) throws Exception {

        // 参数检查：运行该 Driver 时必须传入两个参数：
        // args[0]：输入路径
        // args[1]：输出路径
        if (args.length != 2) {
            System.err.println("Usage: FactorJob1Driver <input> <output>");
            System.exit(1);
        }

        // Hadoop 配置对象：
        // Configuration 会加载 Hadoop 的配置文件，
        // 也可以在这里通过 conf.set(...) 进行参数调优（本代码未设置额外参数）。
        Configuration conf = new Configuration();

        /* =========================
         * [TUNING] Shuffle / Spill 提速配置（不改变计算逻辑）
         * ========================= */

// Map 端排序缓冲区（默认 100MB → 256MB，减少 spill）
        conf.setInt("mapreduce.task.io.sort.mb", 256);

// spill 触发比例（ 0.90，必须是 float）
        conf.setFloat("mapreduce.map.sort.spill.percent", 0.90f);

// Reduce 端并发拉取 map 输出（10）
        conf.setInt("mapreduce.reduce.shuffle.parallelcopies", 10);

// Mapper → Reducer 中间结果压缩（只影响 shuffle）

        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "factor_job1");


        // 指定该 Job 的 jar 包入口类：
        // Hadoop 在集群运行时需要把包含 Driver/Mapper/Reducer 的 jar 分发到各节点，
        // setJarByClass 用于告诉 Hadoop 从哪个类所在的 jar 作为提交包。
        job.setJarByClass(FactorJob1Driver.class);

        // =========================
        // Mapper 配置
        // =========================

        // 设置 Mapper 类：
        // DataCleanMapper 负责读取原始输入，进行字段解析/清洗，
        // 然后输出中间键值对。
        job.setMapperClass(DataCleanMapper.class);

        // 设置 Mapper 输出的 key 类型：
        // Text 是 Hadoop 的字符串序列化类型（比 Java String 更适合 MR 传输）。
        job.setMapOutputKeyClass(Text.class);

        // 设置 Mapper 输出的 value 类型：
        // StockBean 是自定义的 Writable（可序列化对象），用于承载一条结构化后的股票快照数据。
        job.setMapOutputValueClass(StockBean.class);

        //开启 Mapper → Reducer 的“中间结果”压缩（只影响 shuffle / spill）
        conf.setBoolean("mapreduce.map.output.compress", true);
        //压缩算法：Snappy 速度快（省网络/磁盘 IO）
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");


        // =========================
        // Reducer 配置
        // =========================

        // 设置 Reducer 类：
        // FactorCalcReducer 接收 Mapper 输出，按相同 key 进行聚合处理，
        // 并生成最终输出（OutputFormat 写到 HDFS）。
        job.setReducerClass(FactorCalcReducer.class);

        // 设置 Reducer 最终输出的 key 类型：Text。
        job.setOutputKeyClass(Text.class);

        // 设置 Reducer 最终输出的 value 类型：
        // 这里输出 value 为 Text，表示 Reducer 会把结果拼成文本。
        job.setOutputValueClass(Text.class);

        //设置4个Reducer，按照docker CPU核心数4，多线程跑。
        job.setNumReduceTasks(4);


        // =========================
        // 输入输出路径配置
        // =========================

        // 允许递归读取输入目录：
        // 当输入路径 args[0] 是一个包含多天/多文件夹的目录时，开启递归可自动读取子目录下所有文件。
        FileInputFormat.setInputDirRecursive(job, true);

        // 设置输入路径：
        // args[0] 在运行时传入，支持目录或文件。
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置输出路径：
        // args[1] 在运行时传入，必须是一个不存在的目录，否则 Job 会直接失败。
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交 Job 并等待执行完成：
        // waitForCompletion(true) 表示输出进度日志到控制台。
        // 成功则退出码 0，失败退出码 1，便于脚本/流水线判断运行状态。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
