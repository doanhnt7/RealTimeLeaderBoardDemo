package jobs.models;

public class TeamMvp {
    private String teamId;
    private String mvpUserId;
    private double mvpUserTotalScore;
    private double teamTotalScore;
    private double currentContributionRatio;
    private long lastUpdateTime;

    public TeamMvp() {}

    public TeamMvp(String teamId, String mvpUserId, double mvpUserTotalScore,
                   double teamTotalScore, double currentContributionRatio,
                   Double previousContributionRatio, long lastUpdateTime) {
        this.teamId = teamId;
        this.mvpUserId = mvpUserId;
        this.mvpUserTotalScore = mvpUserTotalScore;
        this.teamTotalScore = teamTotalScore;
        this.currentContributionRatio = currentContributionRatio;
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getTeamId() { return teamId; }
    public String getMvpUserId() { return mvpUserId; }
    public double getMvpUserTotalScore() { return mvpUserTotalScore; }
    public double getTeamTotalScore() { return teamTotalScore; }
    public double getCurrentContributionRatio() { return currentContributionRatio; }
    public long getLastUpdateTime() { return lastUpdateTime; }

    public void setTeamId(String teamId) { this.teamId = teamId; }
    public void setMvpUserId(String mvpUserId) { this.mvpUserId = mvpUserId; }
    public void setMvpUserTotalScore(double mvpUserTotalScore) { this.mvpUserTotalScore = mvpUserTotalScore; }
    public void setTeamTotalScore(double teamTotalScore) { this.teamTotalScore = teamTotalScore; }
    public void setCurrentContributionRatio(double currentContributionRatio) { this.currentContributionRatio = currentContributionRatio; }
    public void setLastUpdateTime(long lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
}


