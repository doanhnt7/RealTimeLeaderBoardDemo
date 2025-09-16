package flinkfintechpoc.processors;

import flinkfintechpoc.models.Merchant;
import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.MerchantAnalyticsMetrics;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Merchant Performance Processor - Keyed Process Function
 * Analyzes merchant performance using transaction data and merchant reference data
 * Loads merchant data from PostgreSQL in open() method to avoid race conditions
 */
public class MerchantPerformanceProcessor extends KeyedProcessFunction<String, Transaction, MerchantAnalyticsMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MerchantPerformanceProcessor.class);
    
    // PostgreSQL connection configuration
    private static final String POSTGRES_URL = "jdbc:postgresql://postgres:5432/fintech_demo";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";

    // Local cache for merchant data
    private Map<String, Merchant> merchantCache;
    
    // State for merchant analytics
    private ValueState<Integer> transactionCount;
    private ValueState<BigDecimal> totalAmount;
    private ValueState<Date> lastUpdateTime;
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Load all merchants from PostgreSQL into local cache
        merchantCache = loadMerchantsFromPostgreSQL();
        LOG.info("Loaded {} merchants into cache", merchantCache.size());
        
        // Initialize state descriptors
        ValueStateDescriptor<Integer> transactionCountDescriptor = 
            new ValueStateDescriptor<>("transaction-count", Integer.class);
        ValueStateDescriptor<BigDecimal> totalAmountDescriptor = 
            new ValueStateDescriptor<>("total-amount", BigDecimal.class);
        ValueStateDescriptor<Date> lastUpdateTimeDescriptor = 
            new ValueStateDescriptor<>("last-update-time", Date.class);
        
        transactionCount = getRuntimeContext().getState(transactionCountDescriptor);
        totalAmount = getRuntimeContext().getState(totalAmountDescriptor);
        lastUpdateTime = getRuntimeContext().getState(lastUpdateTimeDescriptor);
    }
    
    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<MerchantAnalyticsMetrics> out) throws Exception {
        // Process transaction and enrich with merchant data
        String merchantId = transaction.getMerchantId();
        if (merchantId == null) return;
        
        // Direct lookup from local cache - no broadcast state needed!
        Merchant merchant = merchantCache.get(merchantId);
        
        if (merchant == null) {
            LOG.warn("Merchant not found in cache for ID: {}", merchantId);
            return;
        }
        
        // Update analytics state
        Integer currentCount = transactionCount.value();
        BigDecimal currentTotal = totalAmount.value();
        Date currentTime = transaction.getCreatedAt();
        
        if (currentCount == null) currentCount = 0;
        if (currentTotal == null) currentTotal = BigDecimal.ZERO;
        
        // Update state
        transactionCount.update(currentCount + 1);
        totalAmount.update(currentTotal.add(transaction.getAmount()));
        lastUpdateTime.update(currentTime);
        
        // Calculate analytics
        int totalTransactions = currentCount + 1;
        BigDecimal totalAmountValue = currentTotal.add(transaction.getAmount());
        BigDecimal averageAmount = totalAmountValue.divide(BigDecimal.valueOf(totalTransactions), 2, java.math.RoundingMode.HALF_UP);
        
        String performanceLevel = calculatePerformanceLevel(totalTransactions, totalAmountValue.doubleValue());
        String riskLevel = calculateRiskLevel(merchantId, totalTransactions, totalAmountValue.doubleValue());
        
        // Extract merchant details for dashboard
        String merchantName = merchant.getName();
        String businessType = merchant.getBusinessType();
        String country = extractCountryFromMerchant(merchant);
        String mccCode = merchant.getMccCode();
        boolean isActive = merchant.isActive();
        
        // Create and emit metrics
        MerchantAnalyticsMetrics metrics = new MerchantAnalyticsMetrics(
            merchantId,
            totalTransactions,
            totalAmountValue.doubleValue(),
            averageAmount.doubleValue(),
            performanceLevel,
            riskLevel
        );
        
        // Set additional merchant details for dashboard
        metrics.setMerchantName(merchantName);
        metrics.setBusinessType(businessType);
        metrics.setCountry(country);
        metrics.setMccCode(mccCode);
        metrics.setActive(isActive);
        
        // Set eventTime to transaction's createdAt time
        metrics.setEventTime(transaction.getCreatedAt());
        
        out.collect(metrics);
        
        LOG.info("Processed transaction for merchant {}: {} transactions, total: ${}", 
                merchantId, totalTransactions, totalAmountValue);
    }
    
    /**
     * Load all merchants from PostgreSQL into a Map for fast lookup
     */
    private Map<String, Merchant> loadMerchantsFromPostgreSQL() {
        Map<String, Merchant> merchants = new HashMap<>();
        String sql = "SELECT id, name, business_type, address, mcc_code, " +
                    "is_active, created_at, updated_at, version " +
                    "FROM merchants ORDER BY id";
        
        try (Connection connection = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
             PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                Merchant merchant = new Merchant();
                merchant.setId(rs.getString("id"));
                merchant.setName(rs.getString("name"));
                merchant.setBusinessType(rs.getString("business_type"));
                merchant.setMccCode(rs.getString("mcc_code"));
                merchant.setActive(rs.getBoolean("is_active"));
                merchant.setCreatedAt(rs.getTimestamp("created_at"));
                merchant.setUpdatedAt(rs.getTimestamp("updated_at"));
                merchant.setVersion(rs.getInt("version"));
                
                // Handle JSONB address field
                String addressJson = rs.getString("address");
                if (addressJson != null && !addressJson.isEmpty()) {
                    // Simple JSON parsing for address - in production use proper JSON parser
                    try {
                        java.util.Map<String, Object> address = new java.util.HashMap<>();
                        // Basic parsing - you might want to use Jackson for proper JSON parsing
                        merchant.setAddress(address);
                    } catch (Exception e) {
                        LOG.warn("Failed to parse address JSON for merchant {}: {}", merchant.getId(), e.getMessage());
                    }
                }
                
                merchants.put(merchant.getId(), merchant);
            }
            
            LOG.info("Successfully loaded {} merchants from PostgreSQL", merchants.size());
        } catch (SQLException e) {
            LOG.error("Error loading merchants from PostgreSQL", e);
            throw new RuntimeException("Failed to load merchants from PostgreSQL", e);
        }
        
        return merchants;
    }

    
    private String calculatePerformanceLevel(int transactionCount, double totalAmount) {
        if (transactionCount > 1000 && totalAmount > 100000) {
            return "HIGH_PERFORMANCE";
        } else if (transactionCount > 500 && totalAmount > 50000) {
            return "MEDIUM_PERFORMANCE";
        } else if (transactionCount > 100 && totalAmount > 10000) {
            return "LOW_PERFORMANCE";
        } else {
            return "MINIMAL_ACTIVITY";
        }
    }
    
    private String calculateRiskLevel(String merchantId, int transactionCount, double totalAmount) {
        // Simple risk calculation based on transaction patterns
        double averageAmount = totalAmount / transactionCount;
        
        if (averageAmount > 10000) {
            return "HIGH_RISK";
        } else if (averageAmount > 5000) {
            return "MEDIUM_RISK";
        } else {
            return "LOW_RISK";
        }
    }
    
    private String extractCountryFromMerchant(Merchant merchant) {
        // Extract country from merchant address
        if (merchant.getAddress() != null) {
            Object countryObj = merchant.getAddress().get("country");
            if (countryObj != null) {
                return countryObj.toString();
            }
        }
        return "UNKNOWN";
    }

}
