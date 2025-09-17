package jobs.models;

public class HotStat {
	private String userId; private double shortAvg; private double longAvg; private double peakHotness;
	public HotStat(String userId, double shortAvg, double longAvg, double peakHotness) { this.userId = userId; this.shortAvg = shortAvg; this.longAvg = longAvg; this.peakHotness = peakHotness; }
	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public double getShortAvg() { return shortAvg; }
	public void setShortAvg(double shortAvg) { this.shortAvg = shortAvg; }
	public double getLongAvg() { return longAvg; }
	public void setLongAvg(double longAvg) { this.longAvg = longAvg; }
	public double getPeakHotness() { return peakHotness; }
	public void setPeakHotness(double peakHotness) { this.peakHotness = peakHotness; }
}
