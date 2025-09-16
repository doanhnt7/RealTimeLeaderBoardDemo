package flinkfintechpoc.processors;

import flinkfintechpoc.models.TransactionMetrics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Customer Transaction Metrics Processor - Optimized with AggregateFunction + ProcessWindowFunction
 * Uses incremental aggregation to handle large windows efficiently
 * Analyzes transaction metrics for each customer within time windows
 */
public class CustomerTransactionMetricsProcessor extends ProcessWindowFunction<EnrichedTransactionAggregator.Accumulator, TransactionMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerTransactionMetricsProcessor.class);
    
    // No keyed state needed for window processing - use local variables instead
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        // No state initialization needed - using local variables for window processing
    }
    
    @Override
    public void process(String customerId, Context context, Iterable<EnrichedTransactionAggregator.Accumulator> accumulators, Collector<TransactionMetrics> out) throws Exception {
        LOG.info("Processing window for customer: {}, window: {} - {}", 
                customerId, context.window().getStart(), context.window().getEnd());
        
        // Get the aggregated accumulator (should be only one due to aggregation)
        EnrichedTransactionAggregator.Accumulator accumulator = accumulators.iterator().next();
        
        LOG.info("Found {} transactions in window for customer {}", accumulator.transactionCount, customerId);
        
        // Generate final metrics for the window
        TransactionMetrics metrics = generateWindowMetrics(customerId, context.window(), accumulator);
        out.collect(metrics);
        
        LOG.info("Emitted TransactionMetrics for customer {}: {} transactions, total: ${}, min: ${}, max: ${}", 
                customerId, accumulator.transactionCount, accumulator.totalAmount, 
                metrics.getMinAmount(), metrics.getMaxAmount());
    }
    
    private TransactionMetrics generateWindowMetrics(String customerId, TimeWindow window, 
            EnrichedTransactionAggregator.Accumulator accumulator) throws Exception {
        
        // Handle null min/max values
        BigDecimal minAmount = accumulator.minAmount;
        BigDecimal maxAmount = accumulator.maxAmount;
        if (minAmount == null) minAmount = BigDecimal.ZERO;
        if (maxAmount == null) maxAmount = BigDecimal.ZERO;
        
        // Calculate derived metrics
        BigDecimal averageAmount = accumulator.transactionCount > 0 ? 
            accumulator.totalAmount.divide(BigDecimal.valueOf(accumulator.transactionCount), 2, java.math.RoundingMode.HALF_UP) : 
            BigDecimal.ZERO;
        String preferredTransactionType = findPreferredTransactionType(accumulator.transactionTypeCounts);
        String preferredLocation = findPreferredLocation(accumulator.locationCounts);
        String preferredDevice = findPreferredDevice(accumulator.deviceCounts);
        
        // Calculate transaction velocity (transactions per seconds) 
        long windowDurationMillis = window.getEnd() - window.getStart();
        double transactionVelocity = windowDurationMillis > 0 ? 
        (accumulator.transactionCount * 1000.0) / windowDurationMillis : 0.0;
        
        // Calculate risk indicators
        double riskScore = calculateSimpleRiskScore(accumulator.totalAmount, accumulator.transactionCount, minAmount, maxAmount);
        
        // Create metrics
        TransactionMetrics metrics = new TransactionMetrics(
            customerId,
            accumulator.accountId, // Use accountId from accumulator
            new Date(window.getEnd()),
            accumulator.totalAmount,
            accumulator.transactionCount,
            averageAmount,
            minAmount,
            maxAmount,
            preferredTransactionType,
            preferredLocation,
            preferredDevice,
            transactionVelocity,
            riskScore
        );
        
        // Set additional metrics
        metrics.setTransactionTypeCounts(accumulator.transactionTypeCounts);
        metrics.setLocationCounts(accumulator.locationCounts);
        metrics.setDeviceCounts(accumulator.deviceCounts);
        metrics.setRecentTransactionCount(accumulator.transactionCount);
        metrics.setLastTransactionAmount(accumulator.lastTransactionAmount); // FIXED: Use actual last transaction
        metrics.setLastTransactionType(accumulator.lastTransactionType); // FIXED: Use actual last transaction type
        
        return metrics;
    }
    
    private String findPreferredTransactionType(Map<String, Integer> typeCounts) {
        return typeCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("unknown");
    }
    
    private String findPreferredLocation(Map<String, Integer> locationCounts) {
        return locationCounts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("unknown");
    }
    
    private String findPreferredDevice(Map<String, Integer> deviceCounts) {
        return deviceCounts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("unknown");
    }
    

    

    
    private double calculateSimpleRiskScore(BigDecimal total, Integer count, BigDecimal min, BigDecimal max) {
        double riskScore = 0.0;
        
        // High total amount risk
        if (total.compareTo(new BigDecimal("100000")) > 0) {
            riskScore += 25.0;
        }
        
        // High frequency risk
        if (count > 50) {
            riskScore += 15.0;
        }
        
        // High individual transaction risk
        if (max.compareTo(new BigDecimal("10000")) > 0) {
            riskScore += 20.0;
        }
        
        // High average amount risk
        if (count > 0) {
            BigDecimal average = total.divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP);
            if (average.compareTo(new BigDecimal("5000")) > 0) {
                riskScore += 15.0;
            }
        }
        
        return Math.min(100.0, riskScore);
    }
}