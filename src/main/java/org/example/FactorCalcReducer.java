package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Job1 的 Reducer：因子计算阶段（单股票时间序列）
 *
 * 输入（来自 DataCleanMapper）：
 *   key   : Text        —— "tradingDay,code"
 *   value : StockBean  —— 该交易日内某只股票的行情快照（3 秒频）
 *
 * 输出：
 *   key   : Text —— "tradingDay,tradeTime,code"
 *   value : Text —— 该股票在该时刻的 20 个 LOB 因子值（逗号分隔）
 *
 * 核心功能：
 * 1) 对同一交易日、同一股票的所有快照按时间排序
 * 2) 基于当前时刻 t 与 t-Δt 的快照计算 20 个高频 LOB 因子
 * 3) 为后续 Job2 的横截面平均提供“单股票-单时刻”的因子序列
 */
public class FactorCalcReducer extends Reducer<Text, StockBean, Text, Text> {

    /**
     * 复用的输出 key/value 对象，避免在 reduce 循环中频繁 new Text，
     * 从而降低GC开销。
     */
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    // 因子计算相关常量

    //前 N 档深度参数
    private static final int N = 5;

    // 时间差参数 Δt
    private static final int DT = 1;

    private static final double EPS = 1e-7;

    @Override
    protected void reduce(Text key, Iterable<StockBean> values, Context context)
            throws IOException, InterruptedException {

        // 1) 拷贝 Iterable 中的 StockBean
        // reduce 阶段复用 value 对象，如果直接保存引用，
        // 后续迭代会导致数据被覆盖，因此必须显式拷贝。
        List<StockBean> snapshots = new ArrayList<>();
        for (StockBean val : values) {
            StockBean copy = new StockBean();
            copy.copyFrom(val);
            snapshots.add(copy);
        }

        // 若快照数量不足以计算 t-Δt（至少需要 DT+1 个），直接跳过
        if (snapshots.size() <= DT) {
            return;
        }

        // 2) 按 tradeTime 对快照排序
        // StockBean 中的 Comparable 接口，排序后保证时间序列正确，
        // 为后续通过 i 和 i-DT 访问 t 与 t-Δt。
        Collections.sort(snapshots);

        // 3) 解析 Reduce Key
        // key 格式为 "tradingDay,code"
        String ks = key.toString();
        int comma = ks.indexOf(',');
        if (comma <= 0 || comma >= ks.length() - 1) return;

        String tradingDay = ks.substring(0, comma);
        String code = ks.substring(comma + 1);

        // 4) 初始化临时变量
        // 复用 StringBuilder，拼接 20 个因子值输出
        StringBuilder sb = new StringBuilder(256);

        // 存 20 个因子的数组
        double[] f = new double[20];

        // 5) 从第 DT=1 个快照开始计算因子
        for (int i = DT; i < snapshots.size(); i++) {

            // 当前时刻 t
            StockBean current  = snapshots.get(i);

            // 上一时刻 t-Δt
            StockBean previous = snapshots.get(i - DT);


            // 6) 提取订单簿数组（当前 & 上一时刻）
            // 直接拿数组引用，避免反复调用 getter，减少开销
            long[] cAp = current.getAp();  //当前卖价
            long[] cBp = current.getBp();  //当前买价
            long[] cAv = current.getAv();  //当前卖量
            long[] cBv = current.getBv();  //当前买量

            long[] pAp = previous.getAp();  //上时刻
            long[] pBp = previous.getBp();
            long[] pAv = previous.getAv();
            long[] pBv = previous.getBv();

            // 买一 / 卖一
            long ap1 = cAp[0];
            long bp1 = cBp[0];
            long av1 = cAv[0];
            long bv1 = cBv[0];


            // 7) 累计前 N 档统计量（当前 & 上一时刻）
            double sumBidVol  = 0.0;
            double sumAskVol  = 0.0;
            double sumBidW    = 0.0;
            double sumAskW    = 0.0;
            double sumBidInv  = 0.0;
            double sumAskInv  = 0.0;

            double sumBidVolPrev = 0.0;
            double sumAskVolPrev = 0.0;

            for (int k = 0; k < N; k++) {
                long bv = cBv[k];
                long av = cAv[k];
                long bp = cBp[k];
                long ap = cAp[k];

                sumBidVol += bv;
                sumAskVol += av;
                // 前 N 档“量加权价格”的分子：Σ (price * vol)
                sumBidW   += (double) bp * bv;
                sumAskW   += (double) ap * av;
                // k 从 0 开始，因此要 +1。
                sumBidInv += (double) bv / (k + 1);
                sumAskInv += (double) av / (k + 1);

                sumBidVolPrev += pBv[k];
                sumAskVolPrev += pAv[k];
            }

            // 全市场买卖总量
            double tBidVol = current.getTotalBidVol();
            double tAskVol = current.getTotalAskVol();

            // 中间价（当前 & 上一时刻）
            double midCur  = (ap1 + bp1) / 2.0;
            long   ap1Prev = pAp[0];
            long   bp1Prev = pBp[0];
            double midPrev = (ap1Prev + bp1Prev) / 2.0;

            // 深度比（当前 & 上一时刻）
            double depthRatioCur  = sumBidVol     / (sumAskVol     + EPS);
            double depthRatioPrev = sumBidVolPrev / (sumAskVolPrev + EPS);
            double totalDepth = sumBidVol + sumAskVol;


            // 8) 计算 20 个 LOB 因子，依照给出公式，加上EPS

            f[0]  = (double) ap1 - bp1;                               // 最优价差
            f[1]  = f[0] / (midCur + EPS);                            // 相对价差
            f[2]  = midCur;                                           // 中间价

            double bv1d = bv1;
            double av1d = av1;
            f[3]  = (bv1d - av1d) / (bv1d + av1d + EPS);              // 买一不平衡

            f[4]  = (sumBidVol - sumAskVol) / (totalDepth + EPS);    // 多档不平衡
            f[5]  = sumBidVol;                                       // 买方深度
            f[6]  = sumAskVol;                                       // 卖方深度
            f[7]  = sumBidVol - sumAskVol;                            // 深度差
            f[8]  = depthRatioCur;                                   // 深度比
            f[9]  = (tBidVol - tAskVol) / (tBidVol + tAskVol + EPS); // 买卖量平衡指数

            double vwapBid = sumBidW / (sumBidVol + EPS);
            double vwapAsk = sumAskW / (sumAskVol + EPS);
            f[10] = vwapBid;                                         // 买方加权价格
            f[11] = vwapAsk;                                         // 卖方加权价格
            f[12] = (sumBidW + sumAskW) / (totalDepth + EPS);        // 加权中间价
            f[13] = vwapAsk - vwapBid;                                // 加权价差
            f[14] = (sumBidVol - sumAskVol) / N;                     // 买卖密度差

            double num16 = sumBidInv - sumAskInv;
            double den16 = sumBidInv + sumAskInv;
            f[15] = num16 / (den16 + EPS);                            // 买卖不对称度

            f[16] = (double) ap1 - ap1Prev;                           // 最优价变动
            f[17] = midCur - midPrev;                                 // 中间价变动
            f[18] = depthRatioCur - depthRatioPrev;                  // 深度比变动
            f[19] = f[0] / (totalDepth + EPS);                        // 价压指标

            // 9) 输出结果

            // 将 20 个因子写为一行 CSV
            sb.setLength(0);
            for (int idx = 0; idx < 20; idx++) {
                if (idx > 0) sb.append(',');
                sb.append(f[idx]);
            }

            // 输出 key = "tradingDay,tradeTime,code"
            long tradeTime = current.getTradeTime();
            outKey.set(tradingDay + "," + tradeTime + "," + code);

            // 输出 value = 因子值序列
            outValue.set(sb.toString());

            context.write(outKey, outValue);
        }
    }
}
