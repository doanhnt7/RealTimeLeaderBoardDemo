package jobs.models;

// Inner class to represent user score with timestamp and previous score
public class Score {
    private String id;
    private double score;
    private long lastUpdateTime;
    private Double previousScore;

    public Score(String id, double score, long lastUpdateTime) {
        this.id = id;
        this.score = score;
        this.lastUpdateTime = lastUpdateTime;
        this.previousScore = null;
    }

    public Score(String id, double score, Double previousScore, long lastUpdateTime) {
        this.id = id;
        this.score = score;
        this.lastUpdateTime = lastUpdateTime;
        this.previousScore = previousScore;
    }

    public String getId() { return id; }
    public double getScore() { return score; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    public Double getPreviousScore() { return previousScore; }
    public void setPreviousScore(Double previousScore) { this.previousScore = previousScore; }

    @Override
    public String toString() {
        return String.format("Score{id='%s', score=%.2f, lastUpdate=%d, previousScore=%s}",
            id, score, lastUpdateTime, previousScore != null ? String.format("%.2f", previousScore) : "null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Score score1 = (Score) o;

        if (Double.compare(score1.score, score) != 0) return false;
        if (lastUpdateTime != score1.lastUpdateTime) return false;
        if (id != null ? !id.equals(score1.id) : score1.id != null) return false;
        return previousScore != null ? previousScore.equals(score1.previousScore) : score1.previousScore == null;
    }
}
