package flinkfintechpoc.models;

import java.util.Date;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Merchant Analytics Metrics model
 */
public class MerchantAnalyticsMetrics {
    private String merchantId;
    private int transactionCount;
    private double totalAmount;
    private double averageAmount;
    private String performanceLevel;
    private String riskLevel;
    private String merchantName;
    private String businessType;
    private String mccCode;
    private String country;
    private boolean isActive;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date eventTime;
    
    // Constructors
    public MerchantAnalyticsMetrics() {}
    
    public MerchantAnalyticsMetrics(String merchantId, int transactionCount,
                                  double totalAmount, double averageAmount,
                                  String performanceLevel, String riskLevel) {
        this.merchantId = merchantId;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
        this.averageAmount = averageAmount;
        this.performanceLevel = performanceLevel;
        this.riskLevel = riskLevel;
        // eventTime should be set by the processor using transaction.getCreatedAt()
    }
    
    // Getters and Setters
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    
    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) { this.transactionCount = transactionCount; }
    
    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
    
    public double getAverageAmount() { return averageAmount; }
    public void setAverageAmount(double averageAmount) { this.averageAmount = averageAmount; }
    
    public String getPerformanceLevel() { return performanceLevel; }
    public void setPerformanceLevel(String performanceLevel) { this.performanceLevel = performanceLevel; }
    
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    
    public String getMerchantName() { return merchantName; }
    public void setMerchantName(String merchantName) { this.merchantName = merchantName; }
    
    public String getBusinessType() { return businessType; }
    public void setBusinessType(String businessType) { this.businessType = businessType; }
    
    public String getMccCode() { return mccCode; }
    public void setMccCode(String mccCode) { this.mccCode = mccCode; }
    
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
    
    public Date getEventTime() { return eventTime; }
    public void setEventTime(Date eventTime) { this.eventTime = eventTime; }
    
    @Override
    public String toString() {
        return "MerchantAnalyticsMetrics{" +
                "merchantId='" + merchantId + '\'' +
                ", merchantName='" + merchantName + '\'' +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", averageAmount=" + averageAmount +
                ", performanceLevel='" + performanceLevel + '\'' +
                ", riskLevel='" + riskLevel + '\'' +
                ", businessType='" + businessType + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
