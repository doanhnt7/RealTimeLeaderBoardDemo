package jobs.models;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TopNDelta implements Serializable {
	private final List<String> toRemove;
	private final Map<String, Double> toUpsert;

	public TopNDelta(List<String> toRemove, Map<String, Double> toUpsert) {

		this.toRemove = toRemove;
		this.toUpsert = toUpsert;
	}

	public List<String> getToRemove() { return toRemove; }
	public Map<String, Double> getToUpsert() { return toUpsert; }
}


