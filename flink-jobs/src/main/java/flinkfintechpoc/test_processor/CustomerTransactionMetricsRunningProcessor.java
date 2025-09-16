package flinkfintechpoc.test_processor;

import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.TransactionMetrics;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CustomerTransactionMetricsRunningProcessor extends KeyedProcessFunction<String, EnrichedTransaction, TransactionMetrics> {

    private transient ValueState<BigDecimal> totalAmount;
    private transient ValueState<Integer> transactionCount;
    private transient ValueState<BigDecimal> minAmount;
    private transient ValueState<BigDecimal> maxAmount;
    private transient ValueState<BigDecimal> lastTransactionAmount;
    private transient ValueState<String> lastTransactionType;
    private transient ValueState<Long> lastEventTimestamp;

    private transient MapState<String, Integer> transactionTypeCounts;
    private transient MapState<String, Integer> locationCounts;
    private transient MapState<String, Integer> deviceCounts;

    @Override
    public void open(OpenContext openContext) throws Exception {
        totalAmount = getRuntimeContext().getState(new ValueStateDescriptor<>("totalAmount", BigDecimal.class));
        transactionCount = getRuntimeContext().getState(new ValueStateDescriptor<>("transactionCount", Integer.class));
        minAmount = getRuntimeContext().getState(new ValueStateDescriptor<>("minAmount", BigDecimal.class));
        maxAmount = getRuntimeContext().getState(new ValueStateDescriptor<>("maxAmount", BigDecimal.class));
        lastTransactionAmount = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTransactionAmount", BigDecimal.class));
        lastTransactionType = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTransactionType", String.class));
        lastEventTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastEventTimestamp", Long.class));

        transactionTypeCounts = getRuntimeContext().getMapState(new MapStateDescriptor<>("transactionTypeCounts", String.class, Integer.class));
        locationCounts = getRuntimeContext().getMapState(new MapStateDescriptor<>("locationCounts", String.class, Integer.class));
        deviceCounts = getRuntimeContext().getMapState(new MapStateDescriptor<>("deviceCounts", String.class, Integer.class));
    }

    @Override
    public void processElement(EnrichedTransaction value, Context ctx, Collector<TransactionMetrics> out) throws Exception {
        // Initialize per-key state with defaults when first seen
        if (totalAmount.value() == null) {
            totalAmount.update(BigDecimal.ZERO);
        }
        if (transactionCount.value() == null) {
            transactionCount.update(0);
        }
        if (lastTransactionType.value() == null) {
            lastTransactionType.update("unknown");
        }
        BigDecimal amount = value.getAmount() != null ? value.getAmount() : BigDecimal.ZERO;

        BigDecimal baseTotal = totalAmount.value();
        if (baseTotal == null) {
            baseTotal = BigDecimal.ZERO;
        }
        BigDecimal currentTotal = baseTotal.add(amount);
        totalAmount.update(currentTotal);

        Integer count = defaultInteger(transactionCount.value()) + 1;
        transactionCount.update(count);

        BigDecimal currentMin = minAmount.value();
        if (currentMin == null || amount.compareTo(currentMin) < 0) {
            minAmount.update(amount);
        }

        BigDecimal currentMax = maxAmount.value();
        if (currentMax == null || amount.compareTo(currentMax) > 0) {
            maxAmount.update(amount);
        }

        lastTransactionAmount.update(amount);
        lastTransactionType.update(safe(value.getTransactionType()));

        String typeKey = safe(value.getTransactionType());
        incrementMap(transactionTypeCounts, typeKey);

        String locationKey = value.getTransactionLocation() != null ? safe(String.valueOf(value.getTransactionLocation().get("city"))) : "unknown";
        incrementMap(locationCounts, locationKey);

        String deviceKey = safe(value.getDeviceFingerprint());
        incrementMap(deviceCounts, deviceKey);

        long eventTs = value.getCreatedAt() != null ? value.getCreatedAt().getTime() : (value.getUpdatedAt() != null ? value.getUpdatedAt().getTime() : ctx.timerService().currentProcessingTime());
        Long lastTs = lastEventTimestamp.value();
        double velocity = 0.0;
        if (lastTs != null) {
            long delta = Math.max(1L, eventTs - lastTs);
            velocity = 1000.0 / delta;
        }
        lastEventTimestamp.update(eventTs);

        BigDecimal avg = count > 0 ? currentTotal.divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP) : BigDecimal.ZERO;
        BigDecimal min = minAmount.value() != null ? minAmount.value() : BigDecimal.ZERO;
        BigDecimal max = maxAmount.value() != null ? maxAmount.value() : BigDecimal.ZERO;

        String preferredType = preferredKey(transactionTypeCounts);
        String preferredLocation = preferredKey(locationCounts);
        String preferredDevice = preferredKey(deviceCounts);

        double riskScore = calculateSimpleRiskScore(currentTotal, count, min, max);

        TransactionMetrics metrics = new TransactionMetrics(
                value.getCustomerId(),
                value.getFromAccountId(),
                new Date(eventTs),
                currentTotal,
                count,
                avg,
                min,
                max,
                preferredType,
                preferredLocation,
                preferredDevice,
                velocity,
                riskScore
        );

        metrics.setTransactionTypeCounts(copyMap(transactionTypeCounts));
        metrics.setLocationCounts(copyMap(locationCounts));
        metrics.setDeviceCounts(copyMap(deviceCounts));
        metrics.setRecentTransactionCount(Math.min(count, 20));
        metrics.setLastTransactionAmount(lastTransactionAmount.value());
        metrics.setLastTransactionType(lastTransactionType.value());

        out.collect(metrics);
    }

    private void incrementMap(MapState<String, Integer> state, String key) throws Exception {
        Integer c = state.get(key);
        state.put(key, c == null ? 1 : c + 1);
    }

    private String preferredKey(MapState<String, Integer> state) throws Exception {
        String best = null;
        int bestCount = -1;
        for (Map.Entry<String, Integer> e : state.entries()) {
            if (e.getValue() != null && e.getValue() > bestCount) {
                best = e.getKey();
                bestCount = e.getValue();
            }
        }
        return best;
    }

    private Map<String, Integer> copyMap(MapState<String, Integer> state) throws Exception {
        Map<String, Integer> m = new HashMap<>();
        for (Map.Entry<String, Integer> e : state.entries()) {
            m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    private double calculateSimpleRiskScore(BigDecimal total, int count, BigDecimal min, BigDecimal max) {
        double t = total != null ? total.doubleValue() : 0.0;
        double spread = (max != null ? max.doubleValue() : 0.0) - (min != null ? min.doubleValue() : 0.0);
        double c = Math.max(1, count);
        return Math.min(100.0, Math.log1p(t) * 5 + spread / c);
    }


    private Integer defaultInteger(Integer v) {
        return v == null ? 0 : v;
    }

    private String safe(String s) {
        return s == null || s.isEmpty() ? "unknown" : s;
    }
}


