package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Enriched Transaction with Customer data
 * Used as intermediate result in cascade processing
 */
public class EnrichedTransaction {
    private String id;
    private String transactionType;
    private String status;
    private String fromAccountId;
    private String toAccountId;
    private String customerId;
    private BigDecimal amount;
    private String merchantId;
    private Map<String, Object> transactionLocation;
    private String deviceFingerprint;
    private Date createdAt;
    private Date updatedAt;
    private Integer version;
    
    // Customer data (enriched)
    private String customerFirstName;
    private String customerLastName;
    private String customerTier;
    private String customerKycStatus;
    private BigDecimal customerRiskScore;
    
    // Constructors
    public EnrichedTransaction() {}
    
    public EnrichedTransaction(Transaction transaction, Customer customer) {
        // Copy transaction data
        this.id = transaction.getId();
        this.transactionType = transaction.getTransactionType();
        this.status = transaction.getStatus();
        this.fromAccountId = transaction.getFromAccountId();
        this.toAccountId = transaction.getToAccountId();
        this.amount = transaction.getAmount();
        this.merchantId = transaction.getMerchantId();
        this.transactionLocation = transaction.getTransactionLocation();
        this.deviceFingerprint = transaction.getDeviceFingerprint();
        this.createdAt = transaction.getCreatedAt();
        this.updatedAt = transaction.getUpdatedAt();
        this.version = 1; // Default version
        
        // Enrich with customer data
        if (customer != null) {
            this.customerId = customer.getId();
            this.customerFirstName = customer.getFirstName();
            this.customerLastName = customer.getLastName();
            this.customerTier = customer.getTier();
            this.customerKycStatus = customer.getKycStatus();
            this.customerRiskScore = BigDecimal.valueOf(customer.getRiskScore());
        }
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public String getFromAccountId() { return fromAccountId; }
    public void setFromAccountId(String fromAccountId) { this.fromAccountId = fromAccountId; }
    
    public String getToAccountId() { return toAccountId; }
    public void setToAccountId(String toAccountId) { this.toAccountId = toAccountId; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    
    public Map<String, Object> getTransactionLocation() { return transactionLocation; }
    public void setTransactionLocation(Map<String, Object> transactionLocation) { this.transactionLocation = transactionLocation; }
    
    public String getDeviceFingerprint() { return deviceFingerprint; }
    public void setDeviceFingerprint(String deviceFingerprint) { this.deviceFingerprint = deviceFingerprint; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    
    // Customer data getters and setters
    public String getCustomerFirstName() { return customerFirstName; }
    public void setCustomerFirstName(String customerFirstName) { this.customerFirstName = customerFirstName; }
    
    public String getCustomerLastName() { return customerLastName; }
    public void setCustomerLastName(String customerLastName) { this.customerLastName = customerLastName; }
    
    public String getCustomerTier() { return customerTier; }
    public void setCustomerTier(String customerTier) { this.customerTier = customerTier; }
    
    public String getCustomerKycStatus() { return customerKycStatus; }
    public void setCustomerKycStatus(String customerKycStatus) { this.customerKycStatus = customerKycStatus; }
    
    public BigDecimal getCustomerRiskScore() { return customerRiskScore; }
    public void setCustomerRiskScore(BigDecimal customerRiskScore) { this.customerRiskScore = customerRiskScore; }
}
