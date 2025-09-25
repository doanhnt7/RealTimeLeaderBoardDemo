package jobs.models;

import java.io.Serializable;

/**
 * Represents a change event for a score in the top N ranking.
 * Similar to Flink's RowKind but for Score objects.
 */
public class ScoreChangeEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public enum ChangeType {
        INSERT,  // Add this score to the ranking
        DELETE   // Remove this score from the ranking
    }
    
    private final ChangeType changeType;
    private final Score score;
    private final int rank;  // Position in the ranking (1-based)
    
    public ScoreChangeEvent(ChangeType changeType, Score score, int rank) {
        this.changeType = changeType;
        this.score = score;
        this.rank = rank;
    }
    
    public ChangeType getChangeType() {
        return changeType;
    }
    
    public Score getScore() {
        return score;
    }
    
    public int getRank() {
        return rank;
    }
    
    public boolean isInsert() {
        return changeType == ChangeType.INSERT;
    }
    
    public boolean isDelete() {
        return changeType == ChangeType.DELETE;
    }
    
    @Override
    public String toString() {
        return String.format("ScoreChangeEvent{type=%s, score=%s, rank=%d}", 
            changeType, score, rank);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        ScoreChangeEvent that = (ScoreChangeEvent) o;
        
        if (rank != that.rank) return false;
        if (changeType != that.changeType) return false;
        return score != null ? score.equals(that.score) : that.score == null;
    }
    
    @Override
    public int hashCode() {
        int result = changeType != null ? changeType.hashCode() : 0;
        result = 31 * result + (score != null ? score.hashCode() : 0);
        result = 31 * result + rank;
        return result;
    }
}
