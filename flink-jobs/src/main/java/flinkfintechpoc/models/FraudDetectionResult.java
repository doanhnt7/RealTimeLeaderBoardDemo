package flinkfintechpoc.models;

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Fraud Detection Result model
 */
public class FraudDetectionResult {
    private String transactionId;
    private String customerId;
    private List<String> fraudTypes;
    private double riskScore;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date detectionTime;
    private String alertType;
    private Map<String, Object> fraudDetails;
    private boolean isFraudulent;
    private String severity;
    private String recommendation;
    
    // Constructors
    public FraudDetectionResult() {}
    
    public FraudDetectionResult(String transactionId, String customerId, List<String> fraudTypes,
                              double riskScore, Date detectionTime, String alertType,
                              Map<String, Object> fraudDetails) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.fraudTypes = fraudTypes;
        this.riskScore = riskScore;
        this.detectionTime = detectionTime;
        this.alertType = alertType;
        this.fraudDetails = fraudDetails;
        this.isFraudulent = riskScore > 50.0;
        this.severity = determineSeverity(riskScore);
        this.recommendation = generateRecommendation(fraudTypes, riskScore);
    }
    
    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public List<String> getFraudTypes() { return fraudTypes; }
    public void setFraudTypes(List<String> fraudTypes) { this.fraudTypes = fraudTypes; }
    
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
    
    public Date getDetectionTime() { return detectionTime; }
    public void setDetectionTime(Date detectionTime) { this.detectionTime = detectionTime; }
    
    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }
    
    public Map<String, Object> getFraudDetails() { return fraudDetails; }
    public void setFraudDetails(Map<String, Object> fraudDetails) { this.fraudDetails = fraudDetails; }
    
    public boolean isFraudulent() { return isFraudulent; }
    public void setFraudulent(boolean fraudulent) { isFraudulent = fraudulent; }
    
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    
    public String getRecommendation() { return recommendation; }
    public void setRecommendation(String recommendation) { this.recommendation = recommendation; }
    
    private String determineSeverity(double riskScore) {
        if (riskScore >= 80) return "CRITICAL";
        if (riskScore >= 60) return "HIGH";
        if (riskScore >= 40) return "MEDIUM";
        return "LOW";
    }
    
    private String generateRecommendation(List<String> fraudTypes, double riskScore) {
        if (riskScore >= 80) return "BLOCK_TRANSACTION";
        if (riskScore >= 60) return "REQUIRE_ADDITIONAL_VERIFICATION";
        if (riskScore >= 40) return "MONITOR_CLOSELY";
        return "CONTINUE_MONITORING";
    }
    
    @Override
    public String toString() {
        return "FraudDetectionResult{" +
                "transactionId='" + transactionId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", fraudTypes=" + fraudTypes +
                ", riskScore=" + riskScore +
                ", detectionTime=" + detectionTime +
                ", alertType='" + alertType + '\'' +
                ", isFraudulent=" + isFraudulent +
                ", severity='" + severity + '\'' +
                '}';
    }
}
