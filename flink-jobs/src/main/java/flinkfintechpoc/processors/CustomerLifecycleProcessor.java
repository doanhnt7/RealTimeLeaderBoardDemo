package flinkfintechpoc.processors;

import flinkfintechpoc.models.CustomerSession;
import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.CustomerLifecycleMetrics;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Queue;
import java.util.LinkedList;

/**
 * Customer Lifecycle Processor - Step 2 of Cascade Pattern
 * Analyzes customer lifecycle based on EnrichedTransaction + CustomerSession data
 * Input: EnrichedTransaction + CustomerSession (broadcast)
 * Output: CustomerLifecycleMetrics
 */
public class CustomerLifecycleProcessor extends KeyedBroadcastProcessFunction<String, EnrichedTransaction, CustomerSession, CustomerLifecycleMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerLifecycleProcessor.class);
    
    // Broadcast state descriptor for customer session reference data (multiple sessions per customer)
    public static final MapStateDescriptor<String, java.util.List<CustomerSession>> SESSION_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>(
            "session-broadcast-state",
            TypeInformation.of(new TypeHint<String>() {}),
            TypeInformation.of(new TypeHint<java.util.List<CustomerSession>>() {})
        );
    
    // State to track customer lifecycle based on enriched transactions
    private ValueState<BigDecimal> totalAmount;
    private ValueState<Integer> totalTransactions;
    private ValueState<String> currentTier;
    private ValueState<String> currentKycStatus;
    // Last-20 transactions average for tier calculation
    private ValueState<Queue<BigDecimal>> last20TransactionAmounts;
    private ValueState<BigDecimal> last20Sum;
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize state descriptors for customer lifecycle tracking
        ValueStateDescriptor<BigDecimal> totalAmountDescriptor = new ValueStateDescriptor<>(
            "total-amount",
            BigDecimal.class
        );
        totalAmount = getRuntimeContext().getState(totalAmountDescriptor);
        
        ValueStateDescriptor<Integer> totalTransactionsDescriptor = new ValueStateDescriptor<>(
            "total-transactions",
            Integer.class
        );
        totalTransactions = getRuntimeContext().getState(totalTransactionsDescriptor);
        
        ValueStateDescriptor<String> tierDescriptor = new ValueStateDescriptor<>(
            "current-tier",
            String.class
        );
        currentTier = getRuntimeContext().getState(tierDescriptor);
        
        ValueStateDescriptor<String> kycDescriptor = new ValueStateDescriptor<>(
            "current-kyc-status",
            String.class
        );
        currentKycStatus = getRuntimeContext().getState(kycDescriptor);
        // Initialize last-20 transaction amounts state
        ValueStateDescriptor<Queue<BigDecimal>> last20Desc = new ValueStateDescriptor<>(
            "last-20-transaction-amounts",
            TypeInformation.of(new TypeHint<Queue<BigDecimal>>() {})
        );
        last20TransactionAmounts = getRuntimeContext().getState(last20Desc);
        
        
        ValueStateDescriptor<BigDecimal> last20SumDesc = new ValueStateDescriptor<>(
            "last-20-sum",
            BigDecimal.class
        );
        last20Sum = getRuntimeContext().getState(last20SumDesc);
    }
    
    @Override
    public void processElement(EnrichedTransaction enrichedTransaction, ReadOnlyContext ctx, Collector<CustomerLifecycleMetrics> out) throws Exception {
        String customerId = enrichedTransaction.getCustomerId();
        Date currentTime = enrichedTransaction.getCreatedAt();
        
        // Get session info from broadcast state
        ReadOnlyBroadcastState<String, java.util.List<CustomerSession>> sessionState = ctx.getBroadcastState(SESSION_STATE_DESCRIPTOR);
        java.util.List<CustomerSession> customerSessions = sessionState.get(customerId);
        
        // Find matching session for this transaction based on timestamp
        CustomerSession session = findMatchingSession(customerSessions, currentTime);
        
        // Skip transaction if no matching session found (as requested)
        if (session == null) {
            LOG.debug("No matching session found for transaction at {} for customer {}, skipping", 
                     currentTime, customerId);
            return;
        }
        
        // Initialize state if first time
        initializeStateIfNeeded(customerId, currentTime);
        
        // Update transaction-based metrics
        updateTransactionMetrics(enrichedTransaction, currentTime);
        updateLast20Amounts(enrichedTransaction);
        
        // Session data is now used directly from broadcast state for tier calculation
        
        // Analyze customer lifecycle based on enriched transaction and session data
        CustomerLifecycleMetrics metrics = analyzeCustomerLifecycle(enrichedTransaction, session, customerSessions, currentTime, customerId);
        
        if (metrics != null) {
            out.collect(metrics);
               LOG.info("Customer lifecycle update for customer {} (tier: {}, kyc: {}, session: {})",
                       customerId, metrics.getCurrentTier(),
                       metrics.getCurrentKycStatus(), session.getChannel());
        }
    }
    
    @Override
    public void processBroadcastElement(CustomerSession session, Context ctx, Collector<CustomerLifecycleMetrics> out) throws Exception {
        // Update broadcast state with customer session reference data (multiple sessions per customer)
        BroadcastState<String, java.util.List<CustomerSession>> sessionState = ctx.getBroadcastState(SESSION_STATE_DESCRIPTOR);
        
        String customerId = session.getCustomerId();
        java.util.List<CustomerSession> customerSessions = sessionState.get(customerId);
        
        if (customerSessions == null) {
            customerSessions = new java.util.ArrayList<>();
        }
        
        // Add new session to the list
        customerSessions.add(session);
        
        // Keep only last 10 sessions per customer (memory management)
        if (customerSessions.size() > 10) {
            // Sort by startedAt and keep the most recent 10
            customerSessions.sort((s1, s2) -> s1.getStartedAt().compareTo(s2.getStartedAt()));
            customerSessions = customerSessions.subList(customerSessions.size() - 10, customerSessions.size());
        }
        
        // Update state
        sessionState.put(customerId, customerSessions);
        
        LOG.info("Updated session broadcast state: {} - {} ({}) - Total sessions: {}", 
                customerId, session.getChannel(), session.getDeviceType(), customerSessions.size());
    }
    
    /**
     * Find the appropriate session for a transaction based on timestamp matching
     */
    private CustomerSession findMatchingSession(java.util.List<CustomerSession> sessions, Date transactionTime) {
        if (sessions == null || sessions.isEmpty() || transactionTime == null) {
            return null;
        }
        
        for (CustomerSession session : sessions) {
            Date sessionStart = session.getStartedAt();
            Date sessionEnd = session.getEndedAt();
            
            if (sessionStart == null) continue;
            
            // Check if transaction time falls within session time window
            boolean afterStart = transactionTime.compareTo(sessionStart) >= 0;
            boolean beforeEnd = sessionEnd == null || transactionTime.compareTo(sessionEnd) <= 0;
            
            if (afterStart && beforeEnd) {
                return session; // Found matching session
            }
        }
        
        return null; // No matching session found
    }
    
    private void initializeStateIfNeeded(String customerId, Date currentTime) throws Exception {
        if (totalAmount.value() == null) {
            totalAmount.update(BigDecimal.ZERO);
        }
        if (totalTransactions.value() == null) {
            totalTransactions.update(0);
        }
        if (currentTier.value() == null) {
            currentTier.update("basic");
        }
        if (currentKycStatus.value() == null) {
            currentKycStatus.update("pending");
        }
    }
    
    private void updateTransactionMetrics(EnrichedTransaction enrichedTransaction, Date currentTime) throws Exception {
        // Update transaction metrics directly
        BigDecimal currentTotal = totalAmount.value();
        Integer currentCount = totalTransactions.value();
        
        totalAmount.update(currentTotal.add(enrichedTransaction.getAmount()));
        totalTransactions.update(currentCount + 1);
    }
    
    
    private CustomerLifecycleMetrics analyzeCustomerLifecycle(EnrichedTransaction enrichedTransaction, CustomerSession session, java.util.List<CustomerSession> customerSessions, Date currentTime, String customerId) throws Exception {
        BigDecimal totalAmountValue = totalAmount.value();
        Integer count = totalTransactions.value();
        String currentTierValue = currentTier.value();
        String currentKycValue = currentKycStatus.value();
        
        // Determine lifecycle events based on transaction patterns and session data
        boolean isUpgrade = false;
        boolean isDowngrade = false;
        boolean kycCompleted = false;
        boolean sessionInsight = false;
        
        // Check for tier changes based on customer sessions and transaction patterns
        String newTier = determineTierFromSessionsAndTransactions(customerSessions, computeAverageLast20());
        if (!newTier.equals(currentTierValue)) {
            if (isTierUpgrade(currentTierValue, newTier)) {
                isUpgrade = true;
            } else {
                isDowngrade = true;
            }
            currentTier.update(newTier);
        }
        
        // Check for KYC completion based on transaction patterns
        if ("pending".equals(currentKycValue) && count > 10 && totalAmountValue.compareTo(new BigDecimal("10000")) > 0) {
            kycCompleted = true;
            currentKycStatus.update("completed");
        }
        
        // Check for session-based insights
        if (session != null) {
            sessionInsight = true;
        }
        
        // Create lifecycle metrics with session insights
        CustomerLifecycleMetrics metrics = new CustomerLifecycleMetrics(
            customerId,
            newTier,
            currentKycStatus.value(),
            count,
            isUpgrade,
            isDowngrade,
            kycCompleted,
            enrichedTransaction.getCustomerRiskScore() != null ? enrichedTransaction.getCustomerRiskScore().doubleValue() : 0.0
        );
        
        // Set eventTime to enrichedTransaction's createdAt time
        metrics.setEventTime(enrichedTransaction.getCreatedAt());
        
        // Add session insights as additional metadata (if needed)
        if (sessionInsight && session != null) {
            // Log session insights for now - could be extended to store in additional fields
            LOG.info("Session insight for customer {}: channel={}, device={}, actions={}", 
                    customerId, session.getChannel(), 
                    session.getDeviceType(), session.getActionsCount());
        }
        
        return metrics;
    }

    private void updateLast20Amounts(EnrichedTransaction enrichedTransaction) throws Exception {
        // Initialize if needed
        if (last20TransactionAmounts.value() == null) {
            last20TransactionAmounts.update(new LinkedList<>());
        }
        if (last20Sum.value() == null) {
            last20Sum.update(BigDecimal.ZERO);
        }

        Queue<BigDecimal> amounts = last20TransactionAmounts.value();
        BigDecimal sum = last20Sum.value();
        BigDecimal newAmount = enrichedTransaction.getAmount();

        // If queue is full (20 elements), remove the oldest (FIFO)
        if (amounts.size() >= 20) {
            BigDecimal oldestAmount = amounts.poll(); // Remove oldest
            if (oldestAmount != null) {
                sum = sum.subtract(oldestAmount);
            }
        }

        // Add new amount to queue
        amounts.offer(newAmount); // Add to end
        sum = sum.add(newAmount);

        // Update states
        last20TransactionAmounts.update(amounts);
        last20Sum.update(sum);
    }

    private BigDecimal computeAverageLast20() throws Exception {
        Queue<BigDecimal> amounts = last20TransactionAmounts.value();
        BigDecimal sum = last20Sum.value();
        
        if (amounts == null || sum == null || amounts.isEmpty()) {
            return BigDecimal.ZERO;
        }
        
        int count = amounts.size();
        return sum.divide(new BigDecimal(count), 2, java.math.RoundingMode.HALF_UP);
    }

    private String determineTierFromSessionsAndTransactions(java.util.List<CustomerSession> customerSessions, BigDecimal avgAmount) {
        // Base tier from transaction amount
        String baseTier = determineTierFromAverage(avgAmount);
        
        // Adjust tier based on session activity
        if (customerSessions != null && !customerSessions.isEmpty()) {
            int totalSessions = customerSessions.size();
            int totalActions = customerSessions.stream().mapToInt(CustomerSession::getActionsCount).sum();
            
            // Upgrade tier based on session activity
            if (totalSessions >= 5 && totalActions >= 50) {
                return upgradeTier(baseTier);
            } else if (totalSessions >= 3 && totalActions >= 20) {
                return upgradeTier(baseTier);
            }
        }
        
        return baseTier;
    }
    
    private String determineTierFromAverage(BigDecimal avgAmount) {
        if (avgAmount.compareTo(new BigDecimal("5000")) >= 0) {
            return "vip";
        } else if (avgAmount.compareTo(new BigDecimal("1000")) >= 0) {
            return "premium";
        } else if (avgAmount.compareTo(new BigDecimal("200")) >= 0) {
            return "standard";
        } else {
            return "basic";
        }
    }
    
    private String upgradeTier(String currentTier) {
        switch (currentTier.toLowerCase()) {
            case "basic": return "standard";
            case "standard": return "premium";
            case "premium": return "vip";
            case "vip": return "vip"; // Already highest tier
            default: return currentTier;
        }
    }
    
    private boolean isTierUpgrade(String oldTier, String newTier) {
        int oldTierLevel = getTierLevel(oldTier);
        int newTierLevel = getTierLevel(newTier);
        return newTierLevel > oldTierLevel;
    }
    
    private int getTierLevel(String tier) {
        switch (tier.toLowerCase()) {
            case "basic": return 1;
            case "standard": return 2;
            case "premium": return 3;
            case "vip": return 4;
            default: return 0;
        }
    }
}