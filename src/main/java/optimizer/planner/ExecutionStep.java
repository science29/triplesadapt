package optimizer.planner;

import index.MyHashMap;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;

public class ExecutionStep {


    protected  HandStep leftHand;
    protected  HandStep rightHand;
    protected JoinType joinType;
    private int cost;

    public ExecutionStep(JoinType joinType) {
        this.joinType = joinType;

    }


    public void setLeftHand(TriplePattern2 pattern, MyHashMap<Integer, ArrayList<Triple>> index, int firstTripleIndex, int secondTripleIndex){
        this.leftHand = new HandStep(pattern,index,firstTripleIndex,secondTripleIndex);
    }



    public class HandStep{
        protected TriplePattern2 pattern;
        private MyHashMap<Integer, ArrayList<Triple>> index;
        private int firstTripleIndex;
        private int secondTripleIndex;

        public HandStep(TriplePattern2 pattern, MyHashMap<Integer, ArrayList<Triple>> index, int firstTripleIndex, int secondTripleIndex) {
            this.pattern = pattern;
            this.index = index;
            this.firstTripleIndex = firstTripleIndex;
            this.secondTripleIndex = secondTripleIndex;
        }
    }

    public class JoinType{
        public static final int HashJoin = 1;
        public static final int MergeJoin = 2;
    }

}
