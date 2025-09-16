package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Transaction metrics for analytics
 */
public class TransactionMetrics {
    private String customerId;
    private String accountId;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date eventTime;
    private BigDecimal totalAmount;
    private int transactionCount;
    private BigDecimal averageAmount;
    private BigDecimal minAmount;
    private BigDecimal maxAmount;
    private String preferredTransactionType;
    private String preferredLocation;
    private String preferredDevice;
    private double transactionVelocity;
    private double riskScore;
    private Map<String, Integer> transactionTypeCounts;
    private Map<String, Integer> locationCounts;
    private Map<String, Integer> deviceCounts;
    private int recentTransactionCount;
    private BigDecimal lastTransactionAmount;
    private String lastTransactionType;
    
    // Constructors
    public TransactionMetrics() {}
    
    public TransactionMetrics(String customerId, String accountId, Date eventTime, BigDecimal totalAmount,
                            int transactionCount, BigDecimal averageAmount, BigDecimal minAmount,
                            BigDecimal maxAmount, String preferredTransactionType,
                            String preferredLocation, String preferredDevice,
                            double transactionVelocity, double riskScore) {
        this.customerId = customerId;
        this.accountId = accountId;
        this.eventTime = eventTime;
        this.totalAmount = totalAmount;
        this.transactionCount = transactionCount;
        this.averageAmount = averageAmount;
        this.minAmount = minAmount;
        this.maxAmount = maxAmount;
        this.preferredTransactionType = preferredTransactionType;
        this.preferredLocation = preferredLocation;
        this.preferredDevice = preferredDevice;
        this.transactionVelocity = transactionVelocity;
        this.riskScore = riskScore;
    }
    
    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    
    public Date getEventTime() { return eventTime; }
    public void setEventTime(Date eventTime) { this.eventTime = eventTime; }
    
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    
    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) { this.transactionCount = transactionCount; }
    
    public BigDecimal getAverageAmount() { return averageAmount; }
    public void setAverageAmount(BigDecimal averageAmount) { this.averageAmount = averageAmount; }
    
    public BigDecimal getMinAmount() { return minAmount; }
    public void setMinAmount(BigDecimal minAmount) { this.minAmount = minAmount; }
    
    public BigDecimal getMaxAmount() { return maxAmount; }
    public void setMaxAmount(BigDecimal maxAmount) { this.maxAmount = maxAmount; }
    
    public String getPreferredTransactionType() { return preferredTransactionType; }
    public void setPreferredTransactionType(String preferredTransactionType) { this.preferredTransactionType = preferredTransactionType; }
    
    public String getPreferredLocation() { return preferredLocation; }
    public void setPreferredLocation(String preferredLocation) { this.preferredLocation = preferredLocation; }
    
    public String getPreferredDevice() { return preferredDevice; }
    public void setPreferredDevice(String preferredDevice) { this.preferredDevice = preferredDevice; }
    
    public double getTransactionVelocity() { return transactionVelocity; }
    public void setTransactionVelocity(double transactionVelocity) { this.transactionVelocity = transactionVelocity; }
    
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
    
    public Map<String, Integer> getTransactionTypeCounts() { return transactionTypeCounts; }
    public void setTransactionTypeCounts(Map<String, Integer> transactionTypeCounts) { this.transactionTypeCounts = transactionTypeCounts; }
    
    public Map<String, Integer> getLocationCounts() { return locationCounts; }
    public void setLocationCounts(Map<String, Integer> locationCounts) { this.locationCounts = locationCounts; }
    
    public Map<String, Integer> getDeviceCounts() { return deviceCounts; }
    public void setDeviceCounts(Map<String, Integer> deviceCounts) { this.deviceCounts = deviceCounts; }
    
    public int getRecentTransactionCount() { return recentTransactionCount; }
    public void setRecentTransactionCount(int recentTransactionCount) { this.recentTransactionCount = recentTransactionCount; }
    
    public BigDecimal getLastTransactionAmount() { return lastTransactionAmount; }
    public void setLastTransactionAmount(BigDecimal lastTransactionAmount) { this.lastTransactionAmount = lastTransactionAmount; }
    
    public String getLastTransactionType() { return lastTransactionType; }
    public void setLastTransactionType(String lastTransactionType) { this.lastTransactionType = lastTransactionType; }
    
    @Override
    public String toString() {
        return "TransactionMetrics{" +
                "customerId='" + customerId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", eventTime=" + eventTime +
                ", totalAmount=" + totalAmount +
                ", transactionCount=" + transactionCount +
                ", averageAmount=" + averageAmount +
                ", minAmount=" + minAmount +
                ", maxAmount=" + maxAmount +
                ", preferredTransactionType='" + preferredTransactionType + '\'' +
                ", preferredLocation='" + preferredLocation + '\'' +
                ", preferredDevice='" + preferredDevice + '\'' +
                ", transactionVelocity=" + transactionVelocity +
                ", riskScore=" + riskScore +
                '}';
    }
}