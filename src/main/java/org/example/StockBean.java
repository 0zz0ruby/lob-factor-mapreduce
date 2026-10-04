package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * StockBean：清洗后的“单条行情快照”数据封装类
 *
 * 在整体 MapReduce 流程中的作用：
 * - Job1 的 DataCleanMapper 会将原始 CSV 行解析为 StockBean
 * - Job1 的 FactorCalcReducer 会按 (tradingDay, code) 聚合 StockBean，
 *   并按照 tradeTime 排序，用于计算 t 与 t-Δt 的因子
 *
 * 为什么实现 Writable：
 * - Hadoop 在 Map -> Shuffle -> Reduce 过程中需要对 value 进行序列化传输
 * - 自定义对象必须实现 Writable 接口，并实现 write / readFields 方法
 *
 * 性能与健壮性设计点：
 * 1) 不使用 String.split（基于正则，性能差、GC 压力大），而是手写 split
 * 2) 只解析前 37 列（0..36），因为本项目计算因子只用到这些字段
 * 3) 对列数进行校验：不足 37 列直接丢弃，防止数组越界
 *本项目 Job1 因子计算只用到前 N=5 档（FactorCalcReducer 里 N=5），
 * 因此 StockBean “瘦身”为仅保留前 5 档，并且 Writable 序列化也只传前 5 档，
 * 直接减少 Shuffle 传输字节数（Map -> Reduce 中间 value 更小）。
 */
public class StockBean implements Writable, Comparable<StockBean> {

    /** 股票代码（用于分组与最终输出） */
    private String code;

    /** 交易时间 tradeTime（用于排序，以及 t / t-Δt 对齐） */
    private long tradeTime;

    /**
     * 十档订单簿价量数据：
     * - 数组索引 0 表示第 1 档
     * - 数组索引 9 表示第 10 档
     */
    // 固定为 5 档
    private static final int BOOK_DEPTH = 5;

    private long[] bp = new long[BOOK_DEPTH]; // 买价 bid prices
    private long[] bv = new long[BOOK_DEPTH]; // 买量 bid volumes
    private long[] ap = new long[BOOK_DEPTH]; // 卖价 ask prices
    private long[] av = new long[BOOK_DEPTH]; // 卖量 ask volumes

    /** 全市场买单总量（用于买卖量平衡类因子） */
    private long totalBidVol;

    /** 全市场卖单总量（用于买卖量平衡类因子） */
    private long totalAskVol;

    /** 最新成交价（本项目中主要用于扩展或调试） */
    private long lastPrice;

    /**
     * 无参构造函数：
     * Hadoop 在反序列化时会通过反射调用无参构造函数创建对象
     */
    public StockBean() {}

    /**
     * 解析一行 CSV 行为 StockBean
     *
     * 解析规则：
     * - 若行为空 / 为表头，则直接返回 null
     * - 只解析前 37 列（索引 0..36）
     * - 若列数不足 37，视为非法数据，直接丢弃
     *
     * @param line 原始 CSV 行
     * @return 解析成功返回 StockBean，失败返回 null
     */
    public static StockBean parse(String line) {
        if (line == null || line.isEmpty()) return null;

        // 跳过表头行，避免将字段名当作数据解析
        if (line.startsWith("tradingDay")) return null;

        // 手写split ，startIdx=17，每档4列 => 17 + 5*4 = 37 列（索引 0..36）
        final int requiredCols = 17 + BOOK_DEPTH * 4; // = 37
        String[] fields = fastSplitFirstN(line, requiredCols);

        // 至少需要 requiredCols 列，否则后续字段访问会越界
        if (fields.length < requiredCols) return null;

        try {
            StockBean bean = new StockBean();

            // tradeTime：第 1 列
            bean.setTradeTime(Long.parseLong(fields[1].trim()));

            // 股票代码 code：第 4 列
            bean.setCode(fields[4].trim());

            // 全市场买卖量：第 12、13 列
            bean.setTotalBidVol(Long.parseLong(fields[12].trim()));
            bean.setTotalAskVol(Long.parseLong(fields[13].trim()));

            // 最新成交价：第 8 列
            bean.setLastPrice(Long.parseLong(fields[8].trim()));

            /**
             * 十档行情数据：
             * - 从第 17 列开始
             * - 每一档 4 列：bp, bv, ap, av
             * - 共 10 档，对应字段范围 [17 .. 56]
             * 但是只解析前五档，便于提高shuffle速度
             */
            int startIdx = 17;
            for (int i = 0; i < BOOK_DEPTH; i++) {
                int base = startIdx + i * 4;
                bean.bp[i] = Long.parseLong(fields[base].trim());       // 买价
                bean.bv[i] = Long.parseLong(fields[base + 1].trim());   // 买量
                bean.ap[i] = Long.parseLong(fields[base + 2].trim());   // 卖价
                bean.av[i] = Long.parseLong(fields[base + 3].trim());   // 卖量
            }

            // code 不能为空，否则后续分组与输出无意义
            if (bean.code == null || bean.code.isEmpty()) return null;

            return bean;
        } catch (Exception e) {
            // 任意解析异常（格式错误、空字段、数字转换失败等）均视为非法数据
            return null;
        }
    }

    /**
     * 手写 split 方法：只拆分前 nCols 列
     *
     * 设计动机：
     * - String.split(",") 使用正则表达式，性能较差
     * - 本方法通过字符扫描方式拆分，减少对象创建与 GC 压力
     *
     * 行为说明：
     * - 若实际列数 >= nCols：返回长度为 nCols 的数组
     * - 若实际列数 <  nCols：返回真实列数长度的数组
     */
    private static String[] fastSplitFirstN(String s, int nCols) {
        String[] out = new String[nCols];
        int col = 0;
        int start = 0;
        int len = s.length();

        for (int i = 0; i <= len && col < nCols; i++) {
            if (i == len || s.charAt(i) == ',') {
                out[col++] = s.substring(start, i);
                start = i + 1;
            }
        }

        // 实际列数不足 nCols，则返回真实长度数组
        if (col < nCols) {
            String[] smaller = new String[col];
            System.arraycopy(out, 0, smaller, 0, col);
            return smaller;
        }
        return out;
    }

    /**
     * Writable 序列化方法：
     * 按固定顺序将对象字段写入 DataOutput
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(code);
        out.writeLong(tradeTime);
        for (int i = 0; i < BOOK_DEPTH; i++) {
            out.writeLong(bp[i]); out.writeLong(bv[i]);
            out.writeLong(ap[i]); out.writeLong(av[i]);
        }
        out.writeLong(totalBidVol);
        out.writeLong(totalAskVol);
        out.writeLong(lastPrice);
    }

    /**
     * Writable 反序列化方法：
     * 读取顺序必须与 write(...) 完全一致
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.code = in.readUTF();
        this.tradeTime = in.readLong();

        for (int i = 0; i < BOOK_DEPTH; i++) {
            bp[i] = in.readLong(); bv[i] = in.readLong();
            ap[i] = in.readLong(); av[i] = in.readLong();
        }
        totalBidVol = in.readLong();
        totalAskVol = in.readLong();
        lastPrice = in.readLong();
    }

    /**
     * Comparable 接口实现：
     * 按 tradeTime 升序排序，用于时间序列因子计算
     */
    @Override
    public int compareTo(StockBean o) {
        return Long.compare(this.tradeTime, o.tradeTime);
    }

    /**
     * 深拷贝方法：
     * 用于 Reducer 中复制 Hadoop 复用的 value 对象，防止数据被覆盖
     */
    public void copyFrom(StockBean other) {
        this.code = other.code;
        this.tradeTime = other.tradeTime;

        System.arraycopy(other.bp, 0, this.bp, 0, this.bp.length);
        System.arraycopy(other.bv, 0, this.bv, 0, this.bv.length);
        System.arraycopy(other.ap, 0, this.ap, 0, this.ap.length);
        System.arraycopy(other.av, 0, this.av, 0, this.av.length);

        this.totalBidVol = other.totalBidVol;
        this.totalAskVol = other.totalAskVol;
        this.lastPrice = other.lastPrice;
    }


    // Getter / Setter

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public long getTradeTime() { return tradeTime; }
    public void setTradeTime(long tradeTime) { this.tradeTime = tradeTime; }

    public long[] getBp() { return bp; }
    public void setBp(long[] bp) { this.bp = bp; }

    public long[] getBv() { return bv; }
    public void setBv(long[] bv) { this.bv = bv; }

    public long[] getAp() { return ap; }
    public void setAp(long[] ap) { this.ap = ap; }

    public long[] getAv() { return av; }
    public void setAv(long[] av) { this.av = av; }

    public long getTotalBidVol() { return totalBidVol; }
    public void setTotalBidVol(long totalBidVol) { this.totalBidVol = totalBidVol; }

    public long getTotalAskVol() { return totalAskVol; }
    public void setTotalAskVol(long totalAskVol) { this.totalAskVol = totalAskVol; }

    public long getLastPrice() { return lastPrice; }
    public void setLastPrice(long lastPrice) { this.lastPrice = lastPrice; }
}
