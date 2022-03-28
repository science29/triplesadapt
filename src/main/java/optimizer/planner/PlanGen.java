package optimizer.planner;

import QueryStuff.Query;
import triple.TriplePattern2;

import java.util.ArrayList;

public class PlanGen  {


    private ArrayList<Plan> plans;
    private Query query;


    public PlanGen(Query query){
        this.query = query;
    }

     public void generateAllMergePlans(){
         //plans to evaluate each pattern alone, then merge the results.

     }

    public void generateAllHashPlans(){
         //plans to evlauate only one starting pattern, then hash join with the rests.

    }

    public ArrayList<Byte> getPossibleIndexes(TriplePattern2 pattern){
        return null;
    }


}
