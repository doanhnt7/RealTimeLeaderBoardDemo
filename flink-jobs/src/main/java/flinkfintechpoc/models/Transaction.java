package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonNaming;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

/**
 * Transaction model for Flink processing
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Transaction {
    private String id;
    private String transactionType;
    private String status;
    private String fromAccountId;
    private String toAccountId;
    private BigDecimal amount;
    private String merchantId;
    @JsonDeserialize(using = CustomerSession.LocationDeserializer.class)
    private Map<String, Object> transactionLocation;
    private String deviceFingerprint;
    private Date createdAt;
    private Date updatedAt;
    private int version;
    
    // Constructors
    public Transaction() {}
    
    public Transaction(String id, BigDecimal amount, String transactionType) {
        this.id = id;
        this.amount = amount;
        this.transactionType = transactionType;
        this.createdAt = new Date();
        this.updatedAt = new Date();
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
    
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    
    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + id + '\'' +
                ", amount=" + amount +
                ", transactionType='" + transactionType + '\'' +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }

    /**
     * Deserializer that accepts JSON array of strings or a stringified JSON array,
     * and returns a List<String>.
     */
    public static class StringOrArrayStringListDeserializer extends org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer<List<String>> {
        private static final ObjectMapper M = new ObjectMapper();
        private static final TypeReference<List<String>> T = new TypeReference<List<String>>() {};

        @Override
        public List<String> deserialize(JsonParser p, DeserializationContext ctxt) {
            try {
                JsonNode n = p.getCodec().readTree(p);
                if (n == null || n.isNull()) return null;
                if (n.isArray()) return M.convertValue(n, T);
                if (n.isTextual()) {
                    String s = n.asText();
                    if (s == null || s.isEmpty()) return null;
                    return M.readValue(s, T);
                }
                return M.convertValue(n, T);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize compliance_flags", e);
            }
        }
    }
}
