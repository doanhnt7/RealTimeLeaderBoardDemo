package flinkfintechpoc.processors;

import flinkfintechpoc.models.Account;
import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.Transaction;
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
 * Stage 1: Enrich Transaction with Account to obtain customerId and basic account flags.
 * Loads account data from PostgreSQL in open() method to avoid race conditions.
 */
public class AccountEnrichmentProcessor extends KeyedProcessFunction<String, Transaction, EnrichedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(AccountEnrichmentProcessor.class);

    // PostgreSQL connection configuration
    private static final String POSTGRES_URL = "jdbc:postgresql://postgres:5432/fintech_demo";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";

    // Local cache for account data
    private Map<String, Account> accountCache;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Load all accounts from PostgreSQL into local cache
        accountCache = loadAccountsFromPostgreSQL();
        LOG.info("Loaded {} accounts into cache", accountCache.size());
    }

    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<EnrichedTransaction> out) throws Exception {
        String accountId = transaction.getFromAccountId();
        if (accountId == null) {
            LOG.warn("Transaction {} has no fromAccountId; passing through without account enrichment", transaction.getId());
            out.collect(new EnrichedTransaction(transaction, null));
            return;
        }

        // Direct lookup from local cache - no broadcast state needed!
        Account account = accountCache.get(accountId);

        EnrichedTransaction enriched = new EnrichedTransaction(transaction, null);
        if (account != null) {
            // Carry customerId from account into the enriched transaction
            enriched.setCustomerId(account.getCustomerId());
        } else {
            LOG.debug("Account not found in cache for id {}", accountId);
        }

        out.collect(enriched);
    }

    /**
     * Load all accounts from PostgreSQL into a Map for fast lookup
     */
    private Map<String, Account> loadAccountsFromPostgreSQL() {
        Map<String, Account> accounts = new HashMap<>();
        String sql = "SELECT id, customer_id, account_type, " +
                    "is_active, created_at, updated_at, version " +
                    "FROM accounts ORDER BY id";
        
        try (Connection connection = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
             PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                Account account = new Account();
                account.setId(rs.getString("id"));
                account.setCustomerId(rs.getString("customer_id"));
                account.setAccountType(rs.getString("account_type"));
                account.setIsActive(rs.getLong("is_active"));
                account.setCreatedAt(rs.getTimestamp("created_at"));
                account.setUpdatedAt(rs.getTimestamp("updated_at"));
                account.setVersion(rs.getInt("version"));
                
                accounts.put(account.getId(), account);
            }
            
            LOG.info("Successfully loaded {} accounts from PostgreSQL", accounts.size());
        } catch (SQLException e) {
            LOG.error("Error loading accounts from PostgreSQL", e);
            throw new RuntimeException("Failed to load accounts from PostgreSQL", e);
        }
        
        return accounts;
    }
}


