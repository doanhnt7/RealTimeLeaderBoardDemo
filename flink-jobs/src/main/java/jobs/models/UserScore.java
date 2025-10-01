package jobs.models;

public class UserScore {
    private String userId;
    private String teamId;
    private double totalScore;
    private double currentScore;
    private long lastUpdateTime;

    public UserScore() {}

    public UserScore(String userId, String teamId, double totalScore, double currentScore, long lastUpdateTime) {
        this.userId = userId;
        this.teamId = teamId;
        this.totalScore = totalScore;
        this.currentScore = currentScore;
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getUserId() { return userId; }
    public String getTeamId() { return teamId; }
    public double getTotalScore() { return totalScore; }
    public double getCurrentScore() { return currentScore; }
    public long getLastUpdateTime() { return lastUpdateTime; }

    public void setUserId(String userId) { this.userId = userId; }
    public void setTeamId(String teamId) { this.teamId = teamId; }
    public void setTotalScore(double totalScore) { this.totalScore = totalScore; }
    public void setCurrentScore(double currentScore) { this.currentScore = currentScore; }
    public void setLastUpdateTime(long lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }

    @Override
    public String toString() {
        return "UserScore{" +
                "userId='" + userId + '\'' +
                ", teamId='" + teamId + '\'' +
                ", totalScore=" + totalScore +
                ", currentScore=" + currentScore +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}


