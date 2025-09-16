package flinkfintechpoc.models;

import java.util.Date;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonNaming;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Merchant model for Flink processing
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Merchant {
    private String id;
    private String name;
    private String businessType;
    @JsonDeserialize(using = CustomerSession.LocationDeserializer.class)
    private Map<String, Object> address;
    private boolean isActive;
    private String mccCode;
    private Date createdAt;
    private Date updatedAt;
    private int version;
    
    // Constructors
    public Merchant() {}
    
    public Merchant(String id, String name, String businessType, String mccCode) {
        this.id = id;
        this.name = name;
        this.businessType = businessType;
        this.mccCode = mccCode;
        this.isActive = true;
        this.createdAt = new Date();
        this.updatedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getBusinessType() { return businessType; }
    public void setBusinessType(String businessType) { this.businessType = businessType; }
    
    public String getMccCode() { return mccCode; }
    public void setMccCode(String mccCode) { this.mccCode = mccCode; }
    
    public Map<String, Object> getAddress() { return address; }
    public void setAddress(Map<String, Object> address) { this.address = address; }
    
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    
    @Override
    public String toString() {
        return "Merchant{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", businessType='" + businessType + '\'' +
                ", mccCode='" + mccCode + '\'' +
                ", isActive=" + isActive +
                ", createdAt=" + createdAt +
                '}';
    }
}
