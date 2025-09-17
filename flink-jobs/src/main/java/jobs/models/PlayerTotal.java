package jobs.models;

public class PlayerTotal {
	private String userId;
	private String teamName;
	private long totalScore;
	public PlayerTotal(String userId, String teamName, long totalScore) {
		this.userId = userId; this.teamName = teamName; this.totalScore = totalScore;
	}
	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public String getTeamName() { return teamName; }
	public void setTeamName(String teamName) { this.teamName = teamName; }
	public long getTotalScore() { return totalScore; }
	public void setTotalScore(long totalScore) { this.totalScore = totalScore; }
}
