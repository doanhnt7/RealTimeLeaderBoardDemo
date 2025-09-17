package jobs.models;

public class TeamMvp {
	private String userId; private String teamName; private long playerTotal; private long teamTotal; private double contribRatio;
	public TeamMvp(String userId, String teamName, long playerTotal, long teamTotal, double contribRatio) { this.userId = userId; this.teamName = teamName; this.playerTotal = playerTotal; this.teamTotal = teamTotal; this.contribRatio = contribRatio; }
	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public String getTeamName() { return teamName; }
	public void setTeamName(String teamName) { this.teamName = teamName; }
	public long getPlayerTotal() { return playerTotal; }
	public void setPlayerTotal(long playerTotal) { this.playerTotal = playerTotal; }
	public long getTeamTotal() { return teamTotal; }
	public void setTeamTotal(long teamTotal) { this.teamTotal = teamTotal; }
	public double getContribRatio() { return contribRatio; }
	public void setContribRatio(double contribRatio) { this.contribRatio = contribRatio; }
	public String toJson() { return "{\"user_id\":\""+userId+"\",\"team_name\":\""+teamName+"\",\"player_total\":"+playerTotal+",\"team_total\":"+teamTotal+",\"contrib_ratio\":"+contribRatio+"}"; }
}
