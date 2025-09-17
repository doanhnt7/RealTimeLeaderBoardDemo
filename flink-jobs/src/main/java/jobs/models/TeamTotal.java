package jobs.models;

public class TeamTotal {
	private String teamId;
	private String teamName;
	private long totalScore;
	public TeamTotal(String teamId, String teamName, long totalScore) {
		this.teamId = teamId; this.teamName = teamName; this.totalScore = totalScore;
	}
	public String getTeamId() { return teamId; }
	public void setTeamId(String teamId) { this.teamId = teamId; }
	public String getTeamName() { return teamName; }
	public void setTeamName(String teamName) { this.teamName = teamName; }
	public long getTotalScore() { return totalScore; }
	public void setTotalScore(long totalScore) { this.totalScore = totalScore; }
}
