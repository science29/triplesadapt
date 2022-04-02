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
}
