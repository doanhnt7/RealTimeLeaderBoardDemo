package jobs.models;

// Inner class to represent user score with timestamp
public class Score {
    private String id;
    private double score;
    private long lastUpdateTime;
    
    public Score(String id, double score, long lastUpdateTime) {
        this.id = id;
        this.score = score;
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public void updateScore(double newScore, long updateTime) {
        this.score = newScore;
        this.lastUpdateTime = updateTime;
    }
    
    public String getId() { return id; }
    public double getScore() { return score; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    
    @Override
    public String toString() {
        return String.format("Score{id='%s', score=%d, lastUpdate=%d}", 
            id, score, lastUpdateTime);
    }
}
