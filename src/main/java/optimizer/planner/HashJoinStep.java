package optimizer.planner;

import index.MyHashMap;
import triple.Triple;

import java.util.ArrayList;

public class HashJoinStep extends ExecutionStep {


    public HashJoinStep(JoinType joinType) {
        super(joinType);
    }
}
