# LOB Factor Calculation with Hadoop MapReduce

Java / HDFS / MapReduce 实现的订单簿量化因子计算与性能优化项目。

面向 CSI300 五个交易日约 1,500 个行情快照文件，计算 20 个 LOB（限价订单簿）因子，并输出按交易时刻聚合的横截面均值。项目保留两阶段计算方案与最终单 Job 方案，附带可运行代码、计时脚本和五日正确性核验记录。

## 项目成果

- 通过 Hadoop 日志与计数器定位约 300 个小 Map 任务及两阶段中间结果的处理开销。
- 从两阶段方案改造为单 Job：合并完整股票文件、顺序计算相邻快照因子、Mapper 内预聚合，再由 Reducer 汇总均值。
- 使用必要字段解析、两个快照对象交替复用、数值型时间键及每维 `sum/count`，减少对象分配与中间输出。
- `0102 / 0103 / 0104 / 0105 / 0108` 五天均通过 20 个因子的平均相对误差不超过 1% 检查，覆盖 09:30:00—15:00:00。
- 实测单日单 Job 流程约 17 秒；两阶段调参后 Job 执行合计约 151 秒。两者计时范围不同，不能作为严格同口径的加速倍数。

## 计算流程

```text
单日 HDFS 快照（每只股票一个完整、有序文件）
  → 合并多个完整文件为 Map 输入
  → 顺序解析，计算当前与上一有效快照的 20 个因子
  → Mapper 按 tradeTime 累计每维 sum/count
  → Combiner 合并可加统计量
  → 单 Reducer 求横截面均值
  → 有序 CSV → 导出、计时与正确性核验
```

两阶段方案则先按交易日和股票分组、排序并计算逐股票因子，写入 HDFS；第二个 Job 再计算横截面均值。两阶段的 Java 实现保留在同一源码目录，便于理解改造前后的数据流。

## 运行环境

已记录的服务器实验环境：Ubuntu 22.04、JDK 11、Hadoop 3.3.6、单节点 HDFS/YARN，16 vCPU、32 GiB 内存。

需预先安装并配置 Java、Hadoop、HDFS/YARN、Python 3 和 GNU `time`；单 Job 启用了 Snappy Map 输出压缩，需要 Hadoop 环境支持 Snappy。

## 快速运行

### 1. 编译

在 Linux 或 WSL 中，从仓库根目录执行：

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/path/to/hadoop-3.3.6
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
bash scripts/build.sh
```

输出 `build/lob-factor-mapreduce.jar`。构建不会下载数据、修改集群配置或启动 Hadoop 服务。

### 2. 准备单日输入

原始数据和标准答案不随仓库发布，需要自行准备具有相同字段结构的数据。

```text
data/0102/
  000001/snapshot.csv
  000002/snapshot.csv
  ...
standard/0102.csv
```

```bash
hdfs dfs -mkdir -p /lob-factor/input
hdfs dfs -put data/0102 /lob-factor/input/0102
```

每个股票文件必须按时间非递减排列；第一条有效快照作为历史状态，不输出差分结果。当前优化版一次只能处理一个交易日，不能混合多天，也不适用于将同一股票序列任意拆成多个文件的输入。

### 3. 执行、导出和核验

```bash
run_id=$(date +%Y%m%dT%H%M%S)
mkdir -p results/local
set -o pipefail
bash scripts/benchmark_single_job.sh \
  build/lob-factor-mapreduce.jar \
  /lob-factor/input/0102 \
  "/lob-factor/runs/$run_id/0102" \
  "results/local/0102-$run_id.csv" \
  standard/0102.csv \
  64 2>&1 | tee "results/local/0102-$run_id.log"
```

最后一个参数是合并 split 的最大大小（MiB）；可对照测试 `32 / 64 / 128`。请使用独立输出路径：发布版脚本会拒绝覆盖既有结果。

逐日重复执行其余四天；标准答案仅用于本地核验，不需要导入 HDFS。

### 4. 单独核验已有结果

```bash
python3 scripts/score_single_job.py standard/0102.csv results/local/0102.csv
python3 -m unittest discover -s tests -v
```

评分器核对评测区间内的表头、时间集合、重复时间和有限数值，按时间对齐后计算：

```text
relative_error = abs(standard - actual) / abs(standard)
每个因子的 mean(relative_error) <= 0.01
```

标准值为零时采用保守规则：结果也为零则误差为零，否则为 Infinity。交易区间之外的行不参与评分。没有标准答案时，不能仅凭 Job 成功退出判断因子准确性。

## 实验结果

| 方案 / 日期 | 时间（秒） | 计时范围 |
|---|---:|---|
| 两 Job 基线 / 0102 | 195.036 | 两个 Job 提交至执行结束 |
| 两 Job 调整任务内存后 / 0102 | 151.078 | 两个 Job 提交至执行结束 |
| 单 Job / 0102 | 16.553 | 单 Job 运行、结果导出与正确性核验（含旧输出清理） |
| 单 Job / 0103 | 16.714 | 同上 |
| 单 Job / 0104 | 16.764 | 同上 |
| 单 Job / 0105 | 17.222 | 同上 |
| 单 Job / 0108 | 16.759 | 同上 |

五日单 Job 的记录合计为 84.012 秒，平均约 16.802 秒。单 Job 计时从测试脚本清理上次运行的输出开始，覆盖 Hadoop 作业提交与执行、将 HDFS 结果导出为服务器本地 CSV，以及与标准答案比较；不包含原始数据导入、编译或从服务器下载到个人电脑的时间。两 Job 配置调整是单次前后对照。

时间与核验证据的来源、局限详见 [实验说明](docs/experiments.md)，五天核验原文见 [results/accuracy](results/accuracy)。单 Job 时间来自当时保存的终端输出，五天核验日志是归档结果的重新评分；本仓库不把这些核验日志冒充完整 Job 运行日志。

## 仓库结构

```text
src/main/java/org/example/   单 Job、两 Job 与共享 Writable
scripts/                    构建、单 Job 计时与核验
tests/                      评分器回归测试（合成数据）
results/accuracy/           五日归档结果核验记录
results/benchmark_summary.csv  已记录的服务器耗时
docs/experiments.md          实验设置、计时口径与边界
```
