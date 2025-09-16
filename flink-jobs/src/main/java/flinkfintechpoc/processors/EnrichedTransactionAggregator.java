package flinkfintechpoc.processors;

import flinkfintechpoc.models.EnrichedTransaction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * AggregateFunction variant for EnrichedTransaction, mirroring TransactionAggregator logic.
 */
public class EnrichedTransactionAggregator implements AggregateFunction<EnrichedTransaction, EnrichedTransactionAggregator.Accumulator, EnrichedTransactionAggregator.Accumulator> {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichedTransactionAggregator.class);

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(EnrichedTransaction tx, Accumulator acc) {
        BigDecimal amount = tx.getAmount() == null ? BigDecimal.ZERO : tx.getAmount();
        long timestamp = tx.getCreatedAt() == null ? 0L : tx.getCreatedAt().getTime();

        // Set accountId from first transaction (assuming all transactions in window have same accountId)
        if (acc.accountId == null) {
            acc.accountId = tx.getFromAccountId();
        }

        acc.totalAmount = acc.totalAmount.add(amount);
        acc.transactionCount++;

        if (acc.minAmount == null || amount.compareTo(acc.minAmount) < 0) acc.minAmount = amount;
        if (acc.maxAmount == null || amount.compareTo(acc.maxAmount) > 0) acc.maxAmount = amount;

        if (timestamp > acc.lastTimestamp) {
            acc.lastTimestamp = timestamp;
            acc.lastTransactionAmount = amount;
            acc.lastTransactionType = tx.getTransactionType();
        }

        String type = tx.getTransactionType();
        if (type != null) acc.transactionTypeCounts.put(type, acc.transactionTypeCounts.getOrDefault(type, 0) + 1);

        if (tx.getTransactionLocation() != null) {
            Object c = tx.getTransactionLocation().get("country");
            if (c != null) acc.locationCounts.put(c.toString(), acc.locationCounts.getOrDefault(c.toString(), 0) + 1);
        }

        if (tx.getDeviceFingerprint() != null) {
            String d = tx.getDeviceFingerprint();
            acc.deviceCounts.put(d, acc.deviceCounts.getOrDefault(d, 0) + 1);
        }

        LOG.debug("Aggregated enriched tx {} -> count={} total={}", tx.getId(), acc.transactionCount, acc.totalAmount);
        return acc;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    // chỉ cần thiết nếu dùng session window
    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator m = new Accumulator();
        m.accountId = a.accountId != null ? a.accountId : b.accountId; // Prefer first non-null accountId
        m.totalAmount = a.totalAmount.add(b.totalAmount);
        m.transactionCount = a.transactionCount + b.transactionCount;
        m.minAmount = (a.minAmount == null) ? b.minAmount : (b.minAmount == null ? a.minAmount : (a.minAmount.compareTo(b.minAmount) < 0 ? a.minAmount : b.minAmount));
        m.maxAmount = (a.maxAmount == null) ? b.maxAmount : (b.maxAmount == null ? a.maxAmount : (a.maxAmount.compareTo(b.maxAmount) > 0 ? a.maxAmount : b.maxAmount));
        m.lastTimestamp = Math.max(a.lastTimestamp, b.lastTimestamp);
        if (a.lastTimestamp > b.lastTimestamp) {
            m.lastTransactionAmount = a.lastTransactionAmount;
            m.lastTransactionType = a.lastTransactionType;
        } else {
            m.lastTransactionAmount = b.lastTransactionAmount;
            m.lastTransactionType = b.lastTransactionType;
        }
        m.transactionTypeCounts.putAll(a.transactionTypeCounts);
        b.transactionTypeCounts.forEach((k, v) -> m.transactionTypeCounts.merge(k, v, Integer::sum));
        m.locationCounts.putAll(a.locationCounts);
        b.locationCounts.forEach((k, v) -> m.locationCounts.merge(k, v, Integer::sum));
        m.deviceCounts.putAll(a.deviceCounts);
        b.deviceCounts.forEach((k, v) -> m.deviceCounts.merge(k, v, Integer::sum));
        return m;
    }

    public static class Accumulator {
        public String accountId = null;
        public BigDecimal totalAmount = BigDecimal.ZERO;
        public int transactionCount = 0;
        public BigDecimal minAmount = null;
        public BigDecimal maxAmount = null;
        public long lastTimestamp = 0L;
        public BigDecimal lastTransactionAmount = BigDecimal.ZERO;
        public String lastTransactionType = "unknown";
        public Map<String, Integer> transactionTypeCounts = new HashMap<>();
        public Map<String, Integer> locationCounts = new HashMap<>();
        public Map<String, Integer> deviceCounts = new HashMap<>();
    }
}


