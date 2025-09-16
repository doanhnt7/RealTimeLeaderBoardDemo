package flinkfintechpoc.models;

import java.util.Date;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonNaming;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

/**
 * CustomerSession model for Flink processing
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CustomerSession {
    private String id;
    private String customerId;
    private String sessionId;
    private String channel;
    private String deviceType;
    private Date startedAt;
    private Date endedAt;
    private Integer actionsCount;
    private Integer transactionsCount;
    private Date createdAt;
    private Date updatedAt;
    private int version;
    
    // Constructors
    public CustomerSession() {}
    
    public CustomerSession(String id, String customerId, String sessionId, String channel) {
        this.id = id;
        this.customerId = customerId;
        this.sessionId = sessionId;
        this.channel = channel;
        this.createdAt = new Date();
        this.updatedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    
    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }
    
    public Date getStartedAt() { return startedAt; }
    public void setStartedAt(Date startedAt) { this.startedAt = startedAt; }
    
    public Date getEndedAt() { return endedAt; }
    public void setEndedAt(Date endedAt) { this.endedAt = endedAt; }
    
    public Integer getActionsCount() { return actionsCount; }
    public void setActionsCount(Integer actionsCount) { this.actionsCount = actionsCount; }
    
    public Integer getTransactionsCount() { return transactionsCount; }
    public void setTransactionsCount(Integer transactionsCount) { this.transactionsCount = transactionsCount; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    
    @Override
    public String toString() {
        return "CustomerSession{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", channel='" + channel + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                ", actionsCount=" + actionsCount +
                ", transactionsCount=" + transactionsCount +
                '}';
    }

    /**
     * Custom deserializer that accepts either a JSON object or a JSON string containing an object
     * and returns a Map<String, Object>.
     */
    public static class LocationDeserializer extends JsonDeserializer<Map<String, Object>> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) {
            try {
                JsonNode node = p.getCodec().readTree(p);
                if (node == null || node.isNull()) {
                    return null;
                }
                if (node.isObject()) {
                    return MAPPER.convertValue(node, new TypeReference<Map<String, Object>>() {});
                }
                if (node.isTextual()) {
                    String text = node.asText();
                    if (text == null || text.isEmpty()) {
                        return null;
                    }
                    return MAPPER.readValue(text, new TypeReference<Map<String, Object>>() {});
                }
                // Fallback: try generic conversion
                return MAPPER.convertValue(node, new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize location field", e);
            }
        }
    }
}
