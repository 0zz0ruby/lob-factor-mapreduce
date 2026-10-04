package org.example;

/**
 * Lightweight parser for one raw snapshot CSV row.
 *
 * The optimized single-job path only needs tradeTime, total bid/ask volume,
 * and the first five levels of the order book. Avoiding String[] fields and
 * StockBean allocation removes most per-row garbage from the hot Mapper loop.
 */
final class FastStockSnapshot {
    private static final int BOOK_DEPTH = 5;
    private static final int LAST_REQUIRED_COLUMN = 17 + BOOK_DEPTH * 4 - 1;

    long tradeTime;
    long totalBidVol;
    long totalAskVol;
    final long[] bp = new long[BOOK_DEPTH];
    final long[] bv = new long[BOOK_DEPTH];
    final long[] ap = new long[BOOK_DEPTH];
    final long[] av = new long[BOOK_DEPTH];

    boolean parse(String line) {
        if (line == null || line.isEmpty() || line.startsWith("tradingDay")) {
            return false;
        }

        int column = 0;
        int start = 0;
        int length = line.length();
        try {
            for (int i = 0; i <= length; i++) {
                if (i != length && line.charAt(i) != ',') {
                    continue;
                }

                readSelectedColumn(line, column, start, i);
                if (column >= LAST_REQUIRED_COLUMN) {
                    return true;
                }
                column++;
                start = i + 1;
            }
        } catch (NumberFormatException ex) {
            return false;
        }
        return false;
    }

    private void readSelectedColumn(String line, int column, int start, int end) {
        if (column == 1) {
            tradeTime = parseLong(line, start, end);
        } else if (column == 12) {
            totalBidVol = parseLong(line, start, end);
        } else if (column == 13) {
            totalAskVol = parseLong(line, start, end);
        } else if (column >= 17 && column <= LAST_REQUIRED_COLUMN) {
            int offset = column - 17;
            int level = offset / 4;
            int field = offset % 4;
            long value = parseLong(line, start, end);
            if (field == 0) {
                bp[level] = value;
            } else if (field == 1) {
                bv[level] = value;
            } else if (field == 2) {
                ap[level] = value;
            } else {
                av[level] = value;
            }
        }
    }

    private static long parseLong(String s, int start, int end) {
        while (start < end && s.charAt(start) <= ' ') {
            start++;
        }
        while (end > start && s.charAt(end - 1) <= ' ') {
            end--;
        }
        if (start >= end) {
            throw new NumberFormatException("empty numeric field");
        }

        boolean negative = false;
        if (s.charAt(start) == '-') {
            negative = true;
            start++;
            if (start >= end) {
                throw new NumberFormatException("sign without digits");
            }
        }

        long value = 0L;
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw new NumberFormatException("non-digit in numeric field");
            }
            value = value * 10L + (c - '0');
        }
        return negative ? -value : value;
    }
}
