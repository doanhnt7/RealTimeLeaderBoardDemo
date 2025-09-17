package jobs.models;

public class PlayerTeamTotal {
	private String teamId;
	private String teamName;
	private String userId;
	private long playerTotal;
	public PlayerTeamTotal(String teamId, String teamName, String userId, long playerTotal) {
		this.teamId = teamId; this.teamName = teamName; this.userId = userId; this.playerTotal = playerTotal;
	}
	public String getTeamId() { return teamId; }
	public void setTeamId(String teamId) { this.teamId = teamId; }
	public String getTeamName() { return teamName; }
	public void setTeamName(String teamName) { this.teamName = teamName; }
	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public long getPlayerTotal() { return playerTotal; }
	public void setPlayerTotal(long playerTotal) { this.playerTotal = playerTotal; }
}
