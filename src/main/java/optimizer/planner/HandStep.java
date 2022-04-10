package optimizer.planner;

import java.util.ArrayList;

import triple.TriplePattern2;

public class HandStep {
    private ArrayList<TriplePattern2> pattern;
    private Integer indexType;
    private ArrayList<Integer> IDs;

    public HandStep(ArrayList<TriplePattern2> pattern, Integer indexType, ArrayList<Integer> IDs) {
        this.pattern = pattern;
        this.indexType = indexType;
        this.IDs = IDs; 
    }

	public ArrayList<TriplePattern2> getPattern() {
		return pattern;
	}

	public void setPattern(ArrayList<TriplePattern2> pattern) {
		this.pattern = pattern;
	}

	public Integer getIndexType() {
		return indexType;
	}

	public void setIndexType(Integer indexType) {
		this.indexType = indexType;
	}

	public ArrayList<Integer> getIDs() {
		return IDs;
	}

	public void setIDs(ArrayList<Integer> iDs) {
		IDs = iDs;
	}
}