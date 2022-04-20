package optimizer.planner;

import QueryStuff.Query;
import optimizer.planner.HandStep;
import triple.TriplePattern2;

import java.util.ArrayList;

public class PlanGen implements Cloneable {

	private ArrayList<Plan> plans;
	private Query query;

	public PlanGen(Query query) {
		this.query = query;
	}

	public void generateAllMergePlans() throws CloneNotSupportedException {
		ArrayList<TriplePattern2> tripleList = new ArrayList<TriplePattern2>(); // needs the probably existing List of
		// triple Patterns.
		MergeJoin mergeJoin = new MergeJoin();
		ArrayList<ArrayList<ExecutionStep>> execution = new ArrayList<ArrayList<ExecutionStep>>();
		ArrayList<ArrayList<SeedingIndexes>> MergeTemp = new ArrayList<ArrayList<SeedingIndexes>>();
		ArrayList<SeedingIndexes> plans = new ArrayList<SeedingIndexes>();
		int run = 0;
		for (int i1 = 0; i1 < tripleList.size(); i1++) {
			Plan plan = new Plan();
			TriplePattern2 triplePattern = tripleList.get(i1);
			SeedingIndexes planed = plan.Pruning(i1, triplePattern);
			plans.add(planed);
		}
		mergeJoin.MergePlans(run, plans, tripleList, execution, MergeTemp);

	}

	public void generateAllHashPlans() throws CloneNotSupportedException {
		// plans to evaluate only one starting pattern, then hash join with the rests.
		ArrayList<TriplePattern2> tripleList = new ArrayList<TriplePattern2>(); // needs the probably existing List of
		// triple Patterns.
		HashJoin hashJoin = new HashJoin();
		ArrayList<ArrayList<ExecutionStep>> execution = new ArrayList<ArrayList<ExecutionStep>>();
		ArrayList<ArrayList<SeedingIndexes>> MergeTemp = new ArrayList<ArrayList<SeedingIndexes>>();
		ArrayList<SeedingIndexes> plans = new ArrayList<SeedingIndexes>();
		int run = 0;
		for (int i1 = 0; i1 < tripleList.size(); i1++) {
			Plan plan = new Plan();
			TriplePattern2 triplePattern = tripleList.get(i1);
			SeedingIndexes planed = plan.Pruning(i1, triplePattern);
			plans.add(planed);
		}
		hashJoin.MergePlans(run, plans, tripleList, execution, MergeTemp);

	}

	public ArrayList<Byte> getPossibleIndexes(TriplePattern2 pattern) {
		return null;
	}

}
