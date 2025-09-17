package jobs.models;

public class TopTeam {
	private long rnk; private String teamId; private String teamName; private long totalScore;
	public TopTeam(long rnk, String teamId, String teamName, long totalScore) {
		this.rnk = rnk; this.teamId = teamId; this.teamName = teamName; this.totalScore = totalScore;
	}
	public long getRnk() { return rnk; }
	public void setRnk(long rnk) { this.rnk = rnk; }
	public String getTeamId() { return teamId; }
	public void setTeamId(String teamId) { this.teamId = teamId; }
	public String getTeamName() { return teamName; }
	public void setTeamName(String teamName) { this.teamName = teamName; }
	public long getTotalScore() { return totalScore; }
	public void setTotalScore(long totalScore) { this.totalScore = totalScore; }
	public String toJson() {
		return "{\"rnk\":"+rnk+",\"team_id\":\""+teamId+"\",\"team_name\":\""+teamName+"\",\"total_score\":"+totalScore+"}";
	}
}
