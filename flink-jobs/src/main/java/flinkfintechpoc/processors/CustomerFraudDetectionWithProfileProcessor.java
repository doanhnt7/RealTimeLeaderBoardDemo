package flinkfintechpoc.processors;

import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.FraudDetectionResult;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.Objects;

/**
 * Fraud detection using EnrichedTransaction (has customerId and profile fields).
 * Uses customer profile data directly from enriched transactions for fraud detection.
 */
public class CustomerFraudDetectionWithProfileProcessor extends KeyedProcessFunction<String, EnrichedTransaction, FraudDetectionResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerFraudDetectionWithProfileProcessor.class);


    // State for fraud detection patterns
    private ValueState<List<EnrichedTransaction>> recentTransactions;
    private ListState<Tuple2<Long, BigDecimal>> rolling24hTransactionEntries;
    private ListState<Tuple2<Long, String>> rollingLocationEntries24h;
    private ListState<Tuple2<Long, String>> rollingDeviceEntries24h;
    private ValueState<Date> lastTransactionTime;

    // Fraud detection thresholds
    private static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("10000.00");
    private static final BigDecimal DAILY_LIMIT_THRESHOLD = new BigDecimal("50000.00");
    private static final int MAX_TRANSACTIONS_PER_HOUR = 10;
    private static final int MAX_LOCATIONS_PER_DAY = 10;
    private static final int MAX_DEVICES_PER_DAY = 10;
    private static final long RAPID_TRANSACTION_THRESHOLD = 60000; // 1 minute

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        recentTransactions = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "recent-enriched-transactions", TypeInformation.of(new TypeHint<List<EnrichedTransaction>>() {})
        ));
        rolling24hTransactionEntries = getRuntimeContext().getListState(new ListStateDescriptor<>(
            "rolling-24h-amounts", TypeInformation.of(new TypeHint<Tuple2<Long, BigDecimal>>() {})
        ));
        rollingLocationEntries24h = getRuntimeContext().getListState(new ListStateDescriptor<>(
            "rolling-24h-locations", TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {})
        ));
        rollingDeviceEntries24h = getRuntimeContext().getListState(new ListStateDescriptor<>(
            "rolling-24h-devices", TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {})
        ));
        lastTransactionTime = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "last-enriched-transaction-time", TypeInformation.of(new TypeHint<Date>() {})
        ));
    }

    @Override
    public void processElement(EnrichedTransaction tx, Context ctx, Collector<FraudDetectionResult> out) throws Exception {
        String customerId = tx.getCustomerId();
        if (customerId == null) {
            LOG.warn("EnrichedTransaction {} has no customerId, skipping fraud detection", tx.getId());
            return;
        }

        Date currentTime = tx.getCreatedAt();

        if (recentTransactions.value() == null) recentTransactions.update(new ArrayList<>());
        if (lastTransactionTime.value() == null) lastTransactionTime.update(currentTime);

        // Update histories
        List<EnrichedTransaction> history = recentTransactions.value();
        history.add(tx);
        if (history.size() > 100) history = history.subList(history.size() - 100, history.size());
        recentTransactions.update(history);

        // Update rolling metrics for location and device tracking
        updateRollingMetrics(currentTime, tx);

        // Perform comprehensive fraud detection
        FraudDetectionResult result = performFraudDetection(tx, currentTime, customerId);
        
        if (result != null && result.isFraudulent()) {
            out.collect(result);
            LOG.warn("Fraud detected for customer {}: {} - Risk Score: {}", 
                    customerId, result.getFraudTypes(), result.getRiskScore());
        }

        // Update last transaction time
        lastTransactionTime.update(currentTime);
    }


    private void appendRolling(Date now, BigDecimal amount) throws Exception {
        long ts = now.getTime();
        long cutoff = ts - 24L * 60L * 60L * 1000L;
        List<Tuple2<Long, BigDecimal>> entries = new ArrayList<>();
        for (Tuple2<Long, BigDecimal> e : rolling24hTransactionEntries.get()) entries.add(e);
        entries.add(Tuple2.of(ts, amount == null ? BigDecimal.ZERO : amount));
        List<Tuple2<Long, BigDecimal>> pruned = new ArrayList<>();
        for (Tuple2<Long, BigDecimal> e : entries) if (e.f0 >= cutoff) pruned.add(e);
        rolling24hTransactionEntries.update(pruned);
    }

    private void updateRollingMetrics(Date currentTime, EnrichedTransaction tx) throws Exception {
        // Update rolling transaction amounts
        appendRolling(currentTime, tx.getAmount());
        
        // Update rolling location entries
        if (tx.getTransactionLocation() != null) {
            String country = (String) tx.getTransactionLocation().get("country");
            if (country != null) {
                appendRollingValue(currentTime, country, rollingLocationEntries24h);
            }
        }
        
        // Update rolling device entries
        if (tx.getDeviceFingerprint() != null) {
            appendRollingValue(currentTime, tx.getDeviceFingerprint(), rollingDeviceEntries24h);
        }
    }

    private FraudDetectionResult performFraudDetection(EnrichedTransaction tx, Date currentTime, String customerId) throws Exception {
        List<EnrichedTransaction> history = recentTransactions.value();
        Date lastTime = lastTransactionTime.value();
        
        // Check for various fraud patterns
        List<String> fraudTypes = new ArrayList<>();
        double riskScore = 0.0;
        
        // 1. High amount transaction
        if (tx.getAmount() != null && tx.getAmount().compareTo(HIGH_AMOUNT_THRESHOLD) > 0) {
            fraudTypes.add("HIGH_AMOUNT");
            riskScore += 30.0;
        }
        
        // 2. Daily limit exceeded
        Tuple2<BigDecimal, Integer> stats = computeRolling(currentTime);
        if (stats.f0.compareTo(DAILY_LIMIT_THRESHOLD) > 0) {
            fraudTypes.add("DAILY_LIMIT_EXCEEDED");
            riskScore += 40.0;
        }
        
        // 3. Too many transactions per hour
        long oneHourAgo = currentTime.getTime() - 3600000; // 1 hour
        long recentCount = history.stream()
            .filter(t -> t.getCreatedAt().getTime() > oneHourAgo)
            .count();
        if (recentCount > MAX_TRANSACTIONS_PER_HOUR) {
            fraudTypes.add("FREQUENT_TRANSACTIONS");
            riskScore += 25.0;
        }
        
        // 4. Multiple locations in short time (24h unique count)
        Map<String, Integer> locations = computeRollingCounts(currentTime, rollingLocationEntries24h);
        if (locations.size() > MAX_LOCATIONS_PER_DAY) {
            fraudTypes.add("MULTIPLE_LOCATIONS");
            riskScore += 20.0;
        }
        
        // 5. Multiple devices (24h unique count)
        Map<String, Integer> devices = computeRollingCounts(currentTime, rollingDeviceEntries24h);
        if (devices.size() > MAX_DEVICES_PER_DAY) {
            fraudTypes.add("MULTIPLE_DEVICES");
            riskScore += 15.0;
        }
        
        // 6. Rapid transactions
        if (lastTime != null) {
            long timeDiff = currentTime.getTime() - lastTime.getTime();
            if (timeDiff < RAPID_TRANSACTION_THRESHOLD) {
                fraudTypes.add("RAPID_TRANSACTIONS");
                riskScore += 35.0;
            }
        }
        
        // 7. Unusual transaction patterns
        if (isUnusualPattern(tx, history)) {
            fraudTypes.add("UNUSUAL_PATTERN");
            riskScore += 20.0;
        }
        
        // 8. Customer risk profile adjustment (từ enriched data)
        if (tx.getCustomerRiskScore() != null) {
            double profile = tx.getCustomerRiskScore().doubleValue();
            if (profile >= 80.0) {
                fraudTypes.add("HIGH_RISK_PROFILE");
                riskScore += 25.0;
            } else if (profile >= 60.0) {
                fraudTypes.add("MEDIUM_RISK_PROFILE");
                riskScore += 10.0;
            }
        }
        
        // 9. Customer-based fraud detection (using enriched customer data)
        if (tx.getCustomerTier() != null && "SUSPENDED".equals(tx.getCustomerTier())) {
            fraudTypes.add("SUSPENDED_CUSTOMER");
            riskScore += 40.0;
        }
        
        // Create fraud detection result
        if (!fraudTypes.isEmpty() && riskScore > 100.0) {
            return new FraudDetectionResult(
                tx.getId(),
                customerId,
                fraudTypes,
                riskScore,
                currentTime,
                "FRAUD_DETECTED",
                generateFraudDetails(tx, fraudTypes, riskScore)
            );
        }
        
        return null;
    }
    
    private boolean isUnusualPattern(EnrichedTransaction current, List<EnrichedTransaction> history) {
        if (history.size() < 5) return false;
        
        // Check for unusual time patterns
        long currentHour = current.getCreatedAt().toInstant().atZone(java.time.ZoneId.systemDefault()).getHour();
        
        // Check for unusual amount patterns
        BigDecimal averageAmount = history.stream()
            .map(EnrichedTransaction::getAmount)
            .filter(Objects::nonNull)
            .reduce(BigDecimal.ZERO, BigDecimal::add)
            .divide(BigDecimal.valueOf(history.size()), 2, java.math.RoundingMode.HALF_UP);
        
        return (currentHour < 6 || currentHour > 22) || 
               (current.getAmount() != null && current.getAmount().compareTo(averageAmount.multiply(new BigDecimal("3"))) > 0);
    }
    
    private Map<String, Object> generateFraudDetails(EnrichedTransaction tx, List<String> fraudTypes, double riskScore) {
        Map<String, Object> details = new HashMap<>();
        details.put("fraudTypes", fraudTypes);
        details.put("riskScore", riskScore);
        details.put("transactionAmount", tx.getAmount());
        details.put("transactionType", tx.getTransactionType());
        details.put("location", tx.getTransactionLocation());
        details.put("deviceFingerprint", tx.getDeviceFingerprint());
        details.put("timestamp", tx.getCreatedAt());
        details.put("customerRiskScore", tx.getCustomerRiskScore());
        return details;
    }

    private Tuple2<BigDecimal, Integer> computeRolling(Date now) throws Exception {
        long ts = now.getTime();
        long cutoff = ts - 24L * 60L * 60L * 1000L;
        BigDecimal sum = BigDecimal.ZERO; int count = 0;
        for (Tuple2<Long, BigDecimal> e : rolling24hTransactionEntries.get()) {
            if (e.f0 >= cutoff) { sum = sum.add(e.f1); count++; }
        }
        return Tuple2.of(sum, count);
    }

    private void appendRollingValue(Date currentTime, String value, ListState<Tuple2<Long, String>> target) throws Exception {
        long nowTs = currentTime.getTime();
        long cutoff = nowTs - 24L * 60L * 60L * 1000L;

        List<Tuple2<Long, String>> entries = new ArrayList<>();
        for (Tuple2<Long, String> e : target.get()) entries.add(e);
        entries.add(Tuple2.of(nowTs, value));

        List<Tuple2<Long, String>> pruned = new ArrayList<>();
        for (Tuple2<Long, String> e : entries) if (e.f0 >= cutoff) pruned.add(e);
        target.update(pruned);
    }

    private Map<String, Integer> computeRollingCounts(Date currentTime, ListState<Tuple2<Long, String>> source) throws Exception {
        long nowTs = currentTime.getTime();
        long cutoff = nowTs - 24L * 60L * 60L * 1000L;
        Map<String, Integer> counts = new HashMap<>();
        for (Tuple2<Long, String> e : source.get()) {
            if (e.f0 >= cutoff && e.f1 != null) {
                counts.put(e.f1, counts.getOrDefault(e.f1, 0) + 1);
            }
        }
        return counts;
    }
}


