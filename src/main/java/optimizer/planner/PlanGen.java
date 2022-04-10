package optimizer.planner;

import QueryStuff.Query;
import optimizer.planner.HandStep;
import triple.TriplePattern2;

import java.util.ArrayList;

public class PlanGen implements Cloneable{

	private ArrayList<Plan> plans;
	private Query query;

	public PlanGen(Query query) {
		this.query = query;
	}

	public void generateAllMergePlans() throws CloneNotSupportedException {
	ArrayList<ArrayList<ExecutionStep>> allMergePlansExecutionSteps = new ArrayList<ArrayList<ExecutionStep>>();
	// Generates a list of all Variables their indexes and gives them an ID for
	// identification.

	// Generates the merge Plans
	// i is the starting point for each merge plan, so every starting point is
	// evaluated.
		ArrayList<TriplePattern2> tripleList = new ArrayList<TriplePattern2>();
		ArrayList<SeedingIndexes> plans = new ArrayList<SeedingIndexes>();
		for (int i1 = 0; i1 < tripleList.size(); i1++) {
			Plan plan = new Plan();
			TriplePattern2 triplePattern = tripleList.get(i1);
			SeedingIndexes planed = plan.Pruning(i1, triplePattern);
			plans.add(planed);
		}
		// creates a copy of the plans List, that is changeble for each iteration.
		MergeJoin mergeJoin = new MergeJoin();
		ArrayList<ExecutionStep> execution = new ArrayList<ExecutionStep>();
		ArrayList<ArrayList<SeedingIndexes>> MergeTemp = new ArrayList<ArrayList<SeedingIndexes>>();
		mergeJoin.MergePlans(plans, tripleList, execution, MergeTemp);
		allMergePlansExecutionSteps.add(execution);
}


	public void generateAllHashPlans() {
		// plans to evaluate only one starting pattern, then hash join with the rests.

	}

	public ArrayList<Byte> getPossibleIndexes(TriplePattern2 pattern) {
		return null;
	}

}
