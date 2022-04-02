package optimizer.planner;

import index.MyHashMap;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;

public class ExecutionStep {


    protected  HandStep leftHand;
    protected  HandStep rightHand;
    protected byte joinType; // set to 0 for MergeJoin, otherwise to 1 for HashJoin.
    private int cost;
    private byte sort; // if it needs to be sorted 1, otherwise 0.

    public ExecutionStep(byte joinType, HandStep leftHand, HandStep rightHand, byte sort) {
        this.joinType = joinType;
        this.leftHand = leftHand;
        this.rightHand = rightHand;
        this.sort = sort;

    }
    
//    public void setLeftHand(ArrayList<TriplePattern2> pattern, Integer indexType, ArrayList<Integer> IDs) {
//    	this.leftHand = new HandStep(pattern, indexType, IDs);
//    }
//    
//    public void setRightHand(ArrayList<TriplePattern2> pattern, Integer indexType, ArrayList<Integer> IDs) {
//    	this.rightHand = new HandStep(pattern, indexType, IDs);
//    }
    


//    public void setLeftHand(TriplePattern2 pattern, byte indexType, int firstTripleIndex, int secondTripleIndex){
//        this.leftHand = new HandStep(pattern,indexType,firstTripleIndex,secondTripleIndex);
//    }
//    public void setRightHand(TriplePattern2 pattern, byte indexType, int firstTripleIndex, int secondTripleIndex){
//        this.rightHand = new HandStep(pattern,indexType,firstTripleIndex,secondTripleIndex);
//    }

    

//    public class HandStep{
//        protected TriplePattern2 pattern;
//        private byte indexType;
//        private int firstTripleIndex;
//        private int secondTripleIndex;
//
//        public HandStep(TriplePattern2 pattern, byte indexType, int firstTripleIndex, int secondTripleIndex) {
//            this.pattern = pattern;
//            this.indexType = indexType;
//            this.firstTripleIndex = firstTripleIndex; // is the real tripple index meant or the type?
//            this.secondTripleIndex = secondTripleIndex;
//        }
//    }

//    public class JoinType{
//        public static final int HashJoin = 1;
//        public static final int MergeJoin = 2;
//    }

}
