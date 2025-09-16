package flinkfintechpoc.processors;

import flinkfintechpoc.models.Customer;
import flinkfintechpoc.models.EnrichedTransaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Stage 2: Enrich EnrichedTransaction (with customerId present) with Customer profile fields.
 * Loads customer data from PostgreSQL in open() method to avoid race conditions.
 */
public class CustomerProfileEnrichmentProcessor extends KeyedProcessFunction<String, EnrichedTransaction, EnrichedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerProfileEnrichmentProcessor.class);

    // PostgreSQL connection configuration
    private static final String POSTGRES_URL = "jdbc:postgresql://postgres:5432/fintech_demo";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";

    // Local cache for customer data
    private Map<String, Customer> customerCache;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Load all customers from PostgreSQL into local cache
        customerCache = loadCustomersFromPostgreSQL();
        LOG.info("Loaded {} customers into cache", customerCache.size());
    }

    @Override
    public void processElement(EnrichedTransaction value, Context ctx, Collector<EnrichedTransaction> out) throws Exception {
        String customerId = value.getCustomerId();
        if (customerId == null) {
            // Pass through if no customerId yet
            out.collect(value);
            return;
        }

        // Direct lookup from local cache - no broadcast state needed!
        Customer customer = customerCache.get(customerId);
        if (customer == null) {
            // No profile yet; pass through
            LOG.debug("Customer not found in cache for id {}", customerId);
            out.collect(value);
            return;
        }

        // Construct a new enriched transaction by layering customer fields
        EnrichedTransaction enriched = new EnrichedTransaction();
        enriched.setId(value.getId());
        enriched.setTransactionType(value.getTransactionType());
        enriched.setStatus(value.getStatus());
        enriched.setFromAccountId(value.getFromAccountId());
        enriched.setToAccountId(value.getToAccountId());
        enriched.setAmount(value.getAmount());
        enriched.setMerchantId(value.getMerchantId());
        enriched.setTransactionLocation(value.getTransactionLocation());
        enriched.setDeviceFingerprint(value.getDeviceFingerprint());
        enriched.setCreatedAt(value.getCreatedAt());
        enriched.setUpdatedAt(value.getUpdatedAt());
        enriched.setVersion(value.getVersion());
        enriched.setCustomerId(customer.getId());
        enriched.setCustomerFirstName(customer.getFirstName());
        enriched.setCustomerLastName(customer.getLastName());
        enriched.setCustomerTier(customer.getTier());
        enriched.setCustomerKycStatus(customer.getKycStatus());
        enriched.setCustomerRiskScore(java.math.BigDecimal.valueOf(customer.getRiskScore()));

        out.collect(enriched);
    }

    /**
     * Load all customers from PostgreSQL into a Map for fast lookup
     */
    private Map<String, Customer> loadCustomersFromPostgreSQL() {
        Map<String, Customer> customers = new HashMap<>();
        String sql = "SELECT id, first_name, last_name, " +
                    "tier, risk_score, kyc_status, is_active, created_at, updated_at, version " +
                    "FROM customers ORDER BY id";
        
        try (Connection connection = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
             PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                Customer customer = new Customer();
                customer.setId(rs.getString("id"));
                customer.setFirstName(rs.getString("first_name"));
                customer.setLastName(rs.getString("last_name"));
                customer.setTier(rs.getString("tier"));
                customer.setRiskScore(rs.getDouble("risk_score"));
                customer.setKycStatus(rs.getString("kyc_status"));
                customer.setActive(rs.getBoolean("is_active"));
                customer.setCreatedAt(rs.getTimestamp("created_at"));
                customer.setUpdatedAt(rs.getTimestamp("updated_at"));
                customer.setVersion(rs.getInt("version"));
                
                customers.put(customer.getId(), customer);
            }
            
            LOG.info("Successfully loaded {} customers from PostgreSQL", customers.size());
        } catch (SQLException e) {
            LOG.error("Error loading customers from PostgreSQL", e);
            throw new RuntimeException("Failed to load customers from PostgreSQL", e);
        }
        
        return customers;
    }
}


