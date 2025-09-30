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
        DELETE,   // Remove this score from the ranking
        DELETEALL, // Remove all scores from the ranking
    }
    
    private final ChangeType changeType;
    private final Score score;
    
    public ScoreChangeEvent(ChangeType changeType, Score score) {
        this.changeType = changeType;
        this.score = score;
    }
    public ScoreChangeEvent(ChangeType changeType) {
        this.changeType = changeType;
        this.score = null;
    }

    
    public ChangeType getChangeType() {
        return changeType;
    }
    
    public Score getScore() {
        return score;
    }
    
    public boolean isInsert() {
        return changeType == ChangeType.INSERT;
    }
    
    public boolean isDelete() {
        return changeType == ChangeType.DELETE;
    }
    
    public boolean isDeleteAll() {
        return changeType == ChangeType.DELETEALL;
    }
    
    @Override
    public String toString() {
        return String.format("ScoreChangeEvent{type=%s, score=%s}", 
            changeType, score);
    }
}
