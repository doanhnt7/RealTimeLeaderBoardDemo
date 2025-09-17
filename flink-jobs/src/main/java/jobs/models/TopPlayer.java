package jobs.models;

public class TopPlayer {
	private long rnk; private String userId; private String teamName; private long totalScore;
	public TopPlayer(long rnk, String userId, String teamName, long totalScore) {
		this.rnk = rnk; this.userId = userId; this.teamName = teamName; this.totalScore = totalScore;
	}
	public long getRnk() { return rnk; }
	public void setRnk(long rnk) { this.rnk = rnk; }
	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public String getTeamName() { return teamName; }
	public void setTeamName(String teamName) { this.teamName = teamName; }
	public long getTotalScore() { return totalScore; }
	public void setTotalScore(long totalScore) { this.totalScore = totalScore; }
	public String toJson() {
		return "{\"rnk\":"+rnk+",\"user_id\":\""+userId+"\",\"team_name\":\""+teamName+"\",\"total_score\":"+totalScore+"}";
	}
}
