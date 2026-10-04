package org.example;

/**
 * 20 个 LOB 因子的纯计算函数。
 *
 * 公式与原 FactorCalcReducer 保持一致。把计算从 Reducer 抽出后，单 Job Mapper
 * 可以直接对同一股票文件中相邻的两条有效快照进行流式计算。
 */
public final class FactorMath {

    private static final int BOOK_DEPTH = 5;
    private static final int DIM = 20;
    private static final double EPS = 1e-7;

    private FactorMath() {}

    public static double[] calculate(StockBean current, StockBean previous) {
        double[] factors = new double[DIM];
        calculate(current, previous, factors);
        return factors;
    }

    public static void calculate(StockBean current, StockBean previous, double[] f) {
        if (f == null || f.length < DIM) {
            throw new IllegalArgumentException("factor output array must contain at least 20 elements");
        }

        long[] cAp = current.getAp();
        long[] cBp = current.getBp();
        long[] cAv = current.getAv();
        long[] cBv = current.getBv();

        long[] pAp = previous.getAp();
        long[] pBp = previous.getBp();
        long[] pAv = previous.getAv();
        long[] pBv = previous.getBv();

        long ap1 = cAp[0];
        long bp1 = cBp[0];
        long av1 = cAv[0];
        long bv1 = cBv[0];

        double sumBidVol = 0.0;
        double sumAskVol = 0.0;
        double sumBidWeightedPrice = 0.0;
        double sumAskWeightedPrice = 0.0;
        double sumBidDecay = 0.0;
        double sumAskDecay = 0.0;
        double sumBidVolPrevious = 0.0;
        double sumAskVolPrevious = 0.0;

        for (int level = 0; level < BOOK_DEPTH; level++) {
            long bidVolume = cBv[level];
            long askVolume = cAv[level];

            sumBidVol += bidVolume;
            sumAskVol += askVolume;
            sumBidWeightedPrice += (double) cBp[level] * bidVolume;
            sumAskWeightedPrice += (double) cAp[level] * askVolume;
            sumBidDecay += (double) bidVolume / (level + 1);
            sumAskDecay += (double) askVolume / (level + 1);

            sumBidVolPrevious += pBv[level];
            sumAskVolPrevious += pAv[level];
        }

        double totalDepth = sumBidVol + sumAskVol;
        double midPrice = (ap1 + bp1) / 2.0;
        double previousMidPrice = (pAp[0] + pBp[0]) / 2.0;
        double depthRatio = sumBidVol / (sumAskVol + EPS);
        double previousDepthRatio = sumBidVolPrevious / (sumAskVolPrevious + EPS);
        double totalBidVolume = current.getTotalBidVol();
        double totalAskVolume = current.getTotalAskVol();

        f[0] = (double) ap1 - bp1;
        f[1] = f[0] / (midPrice + EPS);
        f[2] = midPrice;
        f[3] = ((double) bv1 - av1) / ((double) bv1 + av1 + EPS);
        f[4] = (sumBidVol - sumAskVol) / (totalDepth + EPS);
        f[5] = sumBidVol;
        f[6] = sumAskVol;
        f[7] = sumBidVol - sumAskVol;
        f[8] = depthRatio;
        f[9] = (totalBidVolume - totalAskVolume)
                / (totalBidVolume + totalAskVolume + EPS);

        double vwapBid = sumBidWeightedPrice / (sumBidVol + EPS);
        double vwapAsk = sumAskWeightedPrice / (sumAskVol + EPS);
        f[10] = vwapBid;
        f[11] = vwapAsk;
        f[12] = (sumBidWeightedPrice + sumAskWeightedPrice) / (totalDepth + EPS);
        f[13] = vwapAsk - vwapBid;
        f[14] = (sumBidVol - sumAskVol) / BOOK_DEPTH;

        double decayDifference = sumBidDecay - sumAskDecay;
        double decayTotal = sumBidDecay + sumAskDecay;
        f[15] = decayDifference / (decayTotal + EPS);

        f[16] = (double) ap1 - pAp[0];
        f[17] = midPrice - previousMidPrice;
        f[18] = depthRatio - previousDepthRatio;
        f[19] = f[0] / (totalDepth + EPS);
    }

    public static void calculate(FastStockSnapshot current,
                                 FastStockSnapshot previous,
                                 double[] f) {
        if (f == null || f.length < DIM) {
            throw new IllegalArgumentException("factor output array must contain at least 20 elements");
        }

        long[] cAp = current.ap;
        long[] cBp = current.bp;
        long[] cAv = current.av;
        long[] cBv = current.bv;
        long[] pAp = previous.ap;
        long[] pBp = previous.bp;
        long[] pAv = previous.av;
        long[] pBv = previous.bv;

        long ap1 = cAp[0];
        long bp1 = cBp[0];
        long av1 = cAv[0];
        long bv1 = cBv[0];

        double sumBidVol = 0.0;
        double sumAskVol = 0.0;
        double sumBidWeightedPrice = 0.0;
        double sumAskWeightedPrice = 0.0;
        double sumBidDecay = 0.0;
        double sumAskDecay = 0.0;
        double sumBidVolPrevious = 0.0;
        double sumAskVolPrevious = 0.0;

        for (int level = 0; level < BOOK_DEPTH; level++) {
            long bidVolume = cBv[level];
            long askVolume = cAv[level];

            sumBidVol += bidVolume;
            sumAskVol += askVolume;
            sumBidWeightedPrice += (double) cBp[level] * bidVolume;
            sumAskWeightedPrice += (double) cAp[level] * askVolume;
            sumBidDecay += (double) bidVolume / (level + 1);
            sumAskDecay += (double) askVolume / (level + 1);

            sumBidVolPrevious += pBv[level];
            sumAskVolPrevious += pAv[level];
        }

        double totalDepth = sumBidVol + sumAskVol;
        double midPrice = (ap1 + bp1) / 2.0;
        double previousMidPrice = (pAp[0] + pBp[0]) / 2.0;
        double depthRatio = sumBidVol / (sumAskVol + EPS);
        double previousDepthRatio = sumBidVolPrevious / (sumAskVolPrevious + EPS);
        double totalBidVolume = current.totalBidVol;
        double totalAskVolume = current.totalAskVol;

        f[0] = (double) ap1 - bp1;
        f[1] = f[0] / (midPrice + EPS);
        f[2] = midPrice;
        f[3] = ((double) bv1 - av1) / ((double) bv1 + av1 + EPS);
        f[4] = (sumBidVol - sumAskVol) / (totalDepth + EPS);
        f[5] = sumBidVol;
        f[6] = sumAskVol;
        f[7] = sumBidVol - sumAskVol;
        f[8] = depthRatio;
        f[9] = (totalBidVolume - totalAskVolume)
                / (totalBidVolume + totalAskVolume + EPS);

        double vwapBid = sumBidWeightedPrice / (sumBidVol + EPS);
        double vwapAsk = sumAskWeightedPrice / (sumAskVol + EPS);
        f[10] = vwapBid;
        f[11] = vwapAsk;
        f[12] = (sumBidWeightedPrice + sumAskWeightedPrice) / (totalDepth + EPS);
        f[13] = vwapAsk - vwapBid;
        f[14] = (sumBidVol - sumAskVol) / BOOK_DEPTH;

        double decayDifference = sumBidDecay - sumAskDecay;
        double decayTotal = sumBidDecay + sumAskDecay;
        f[15] = decayDifference / (decayTotal + EPS);

        f[16] = (double) ap1 - pAp[0];
        f[17] = midPrice - previousMidPrice;
        f[18] = depthRatio - previousDepthRatio;
        f[19] = f[0] / (totalDepth + EPS);
    }
}
