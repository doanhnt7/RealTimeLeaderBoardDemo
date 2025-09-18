package jobs.models;

import java.time.OffsetDateTime;
import java.util.List;

public class User {
    // Original fields from the incoming stream (keep names as-is)
    private String _id;
    private String uid;
    private String email;
    private String authProvider;
    private String appId;
    private String avatar;
    private String geo;
    private String role;
    private OffsetDateTime lastLoginAt;
    private String name;
    private List<Device> devices;
    private List<Object> resources;
    private OffsetDateTime created_at;
    private OffsetDateTime updated_at;
    private int level;
    private OffsetDateTime updatedAt;
    private int team;
    private int previousLevel;

    // Simple nested type for devices
    public static class Device {
        private String fb_analytics_instance_id;
        private String fb_instance_id;
        private String fcmToken;

        public String getFb_analytics_instance_id() { return fb_analytics_instance_id; }
        public void setFb_analytics_instance_id(String fb_analytics_instance_id) { this.fb_analytics_instance_id = fb_analytics_instance_id; }
        public String getFb_instance_id() { return fb_instance_id; }
        public void setFb_instance_id(String fb_instance_id) { this.fb_instance_id = fb_instance_id; }
        public String getFcmToken() { return fcmToken; }
        public void setFcmToken(String fcmToken) { this.fcmToken = fcmToken; }

        @Override
        public String toString() {
            return "Device{\n" +
                    "  fb_analytics_instance_id='" + fb_analytics_instance_id + "'\n" +
                    "  fb_instance_id='" + fb_instance_id + "'\n" +
                    "  fcmToken='" + fcmToken + "'\n" +
                    '}';
        }
    }

    // Getters/Setters for original fields
    public String get_id() { return _id; }
    public void set_id(String _id) { this._id = _id; }
    public String getUid() { return uid; }
    public void setUid(String uid) { this.uid = uid; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getAuthProvider() { return authProvider; }
    public void setAuthProvider(String authProvider) { this.authProvider = authProvider; }
    public String getAppId() { return appId; }
    public void setAppId(String appId) { this.appId = appId; }
    public String getAvatar() { return avatar; }
    public void setAvatar(String avatar) { this.avatar = avatar; }
    public String getGeo() { return geo; }
    public void setGeo(String geo) { this.geo = geo; }
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public OffsetDateTime getLastLoginAt() { return lastLoginAt; }
    public void setLastLoginAt(OffsetDateTime lastLoginAt) { this.lastLoginAt = lastLoginAt; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public List<Device> getDevices() { return devices; }
    public void setDevices(List<Device> devices) { this.devices = devices; }
    public List<Object> getResources() { return resources; }
    public void setResources(List<Object> resources) { this.resources = resources; }
    public OffsetDateTime getCreated_at() { return created_at; }
    public void setCreated_at(OffsetDateTime created_at) { this.created_at = created_at; }
    public OffsetDateTime getUpdated_at() { return updated_at; }
    public void setUpdated_at(OffsetDateTime updated_at) { this.updated_at = updated_at; }
    public int getLevel() { return level; }
    public void setLevel(int level) { this.level = level; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime updatedAt) { this.updatedAt = updatedAt; }
    public int getTeam() { return team; }
    public void setTeam(int team) { this.team = team; }

    // Compatibility adapter getters used by downstream processors
    public String getUserId() { return uid; }
    public String getTeamId() { return String.valueOf(team); }
    // No synthetic label, just return the raw team identifier as a string
    public String getTeamName() { return String.valueOf(team); }
    public int getScore() { return level; }

    // Event time derived from updatedAt or updated_at
    public long getEventTimeMillis() {
        OffsetDateTime ts = updated_at;
        return ts.toInstant().toEpochMilli();
    }
    public int getPreviousLevel() {
        return previousLevel;
    }

    public void setPreviousLevel(int previousLevel) {
        this.previousLevel = previousLevel;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("User{\n");
        sb.append("  _id='").append(_id).append("'\n");
        sb.append("  uid='").append(uid).append("'\n");
        sb.append("  email='").append(email).append("'\n");
        sb.append("  authProvider='").append(authProvider).append("'\n");
        sb.append("  appId='").append(appId).append("'\n");
        sb.append("  avatar='").append(avatar).append("'\n");
        sb.append("  geo='").append(geo).append("'\n");
        sb.append("  role='").append(role).append("'\n");
        sb.append("  lastLoginAt=").append(lastLoginAt).append("\n");
        sb.append("  name='").append(name).append("'\n");
        sb.append("  devices=").append(devices).append("\n");
        sb.append("  resources=").append(resources).append("\n");
        sb.append("  created_at=").append(created_at).append("\n");
        sb.append("  updated_at=").append(updated_at).append("\n");
        sb.append("  level=").append(level).append("\n");
        sb.append("  updatedAt=").append(updatedAt).append("\n");
        sb.append("  team=").append(team).append("\n");
        sb.append("  previousLevel=").append(previousLevel).append("\n");
        sb.append('}');
        return sb.toString();
    }
}
