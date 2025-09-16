package flinkfintechpoc.models;

import java.util.Date;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Customer Lifecycle Metrics model
 */
public class CustomerLifecycleMetrics {
    private String customerId;
    private String currentTier;
    private String currentKycStatus;
    private int tierChangeCount;
    private boolean isUpgrade;
    private boolean isDowngrade;
    private boolean kycCompleted;
    private double riskScore;
    private long timeSinceLastUpdate;
    private String previousTier;
    private String previousKycStatus;
    private double riskScoreChange;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date eventTime;
    
    // Constructors
    public CustomerLifecycleMetrics() {}
    
    public CustomerLifecycleMetrics(String customerId, String currentTier, 
                                  String currentKycStatus, int tierChangeCount,
                                  boolean isUpgrade, boolean isDowngrade, boolean kycCompleted,
                                  double riskScore) {
        this.customerId = customerId;
        this.currentTier = currentTier;
        this.currentKycStatus = currentKycStatus;
        this.tierChangeCount = tierChangeCount;
        this.isUpgrade = isUpgrade;
        this.isDowngrade = isDowngrade;
        this.kycCompleted = kycCompleted;
        this.riskScore = riskScore;
        // eventTime should be set by the processor using enrichedTransaction.getCreatedAt()
    }
    
    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getCurrentTier() { return currentTier; }
    public void setCurrentTier(String currentTier) { this.currentTier = currentTier; }
    
    public String getCurrentKycStatus() { return currentKycStatus; }
    public void setCurrentKycStatus(String currentKycStatus) { this.currentKycStatus = currentKycStatus; }
    
    public int getTierChangeCount() { return tierChangeCount; }
    public void setTierChangeCount(int tierChangeCount) { this.tierChangeCount = tierChangeCount; }
    
    public boolean isUpgrade() { return isUpgrade; }
    public void setUpgrade(boolean upgrade) { isUpgrade = upgrade; }
    
    public boolean isDowngrade() { return isDowngrade; }
    public void setDowngrade(boolean downgrade) { isDowngrade = downgrade; }
    
    public boolean isKycCompleted() { return kycCompleted; }
    public void setKycCompleted(boolean kycCompleted) { this.kycCompleted = kycCompleted; }
    
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
    
    public long getTimeSinceLastUpdate() { return timeSinceLastUpdate; }
    public void setTimeSinceLastUpdate(long timeSinceLastUpdate) { this.timeSinceLastUpdate = timeSinceLastUpdate; }
    
    public String getPreviousTier() { return previousTier; }
    public void setPreviousTier(String previousTier) { this.previousTier = previousTier; }
    
    public String getPreviousKycStatus() { return previousKycStatus; }
    public void setPreviousKycStatus(String previousKycStatus) { this.previousKycStatus = previousKycStatus; }
    
    public double getRiskScoreChange() { return riskScoreChange; }
    public void setRiskScoreChange(double riskScoreChange) { this.riskScoreChange = riskScoreChange; }
    
    public Date getEventTime() { return eventTime; }
    public void setEventTime(Date eventTime) { this.eventTime = eventTime; }
    
    @Override
    public String toString() {
        return "CustomerLifecycleMetrics{" +
                "customerId='" + customerId + '\'' +
                ", currentTier='" + currentTier + '\'' +
                ", currentKycStatus='" + currentKycStatus + '\'' +
                ", tierChangeCount=" + tierChangeCount +
                ", isUpgrade=" + isUpgrade +
                ", isDowngrade=" + isDowngrade +
                ", kycCompleted=" + kycCompleted +
                ", riskScore=" + riskScore +
                '}';
    }
}
