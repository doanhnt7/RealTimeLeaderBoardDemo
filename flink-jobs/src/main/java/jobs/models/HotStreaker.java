package jobs.models;

public class HotStreaker {
	private long rnk; private String userId; private double shortAvg; private double longAvg; private double peakHotness;
	public HotStreaker(long rnk, String userId, double shortAvg, double longAvg, double peakHotness) 
	{ this.rnk = rnk; this.userId = userId; this.shortAvg = shortAvg; this.longAvg = longAvg; this.peakHotness = peakHotness; }
	public long getRnk() { return rnk; }
	public void setRnk(long rnk) { this.rnk = rnk; }
	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public double getShortAvg() { return shortAvg; }
	public void setShortAvg(double shortAvg) { this.shortAvg = shortAvg; }
	public double getLongAvg() { return longAvg; }
	public void setLongAvg(double longAvg) { this.longAvg = longAvg; }
	public double getPeakHotness() { return peakHotness; }
	public void setPeakHotness(double peakHotness) { this.peakHotness = peakHotness; }
	public String toJson() {
		return "{"
			+ "\"rnk\":" + rnk + ","
			+ "\"user_id\":\"" + userId + "\","
			+ "\"short_term_avg\":" + shortAvg + ","
			+ "\"long_term_avg\":" + longAvg + ","
			+ "\"peak_hotness\":" + peakHotness
			+ "}";
	}
}
