package optimizer.planner;

import QueryStuff.Query;
import optimizer.planner.HandStep;
import triple.TriplePattern2;

import java.util.ArrayList;

public class PlanGen {

	private ArrayList<Plan> plans;
	private Query query;

	public PlanGen(Query query) {
		this.query = query;
	}

	public void generateAllMergePlans() {
		// plans to evaluate each pattern alone, then merge the results.
		ArrayList<TriplePattern2> tripleList = new ArrayList<TriplePattern2>(); // needs the probably existing List of triple Patterns.
		ArrayList<SeedingIndexes> plans = new ArrayList<SeedingIndexes>();
		ArrayList<ArrayList<ExecutionStep>> allMergePlansExecutionSteps = new ArrayList<ArrayList<ExecutionStep>>();
		// Generates a list of all Variables their indexes and gives them an ID for identification.
		for (int i = 0; i < tripleList.size(); i++) {
			Plan plan = new Plan();
			TriplePattern2 triplePattern = tripleList.get(i);
			SeedingIndexes planed = plan.Pruning(i, triplePattern);
			plans.add(planed);
			i++;
		}
		// Generates the merge Plans
		// i is the starting point for each merge plan, so every starting point is evaluated.
		for (int i = 0; i < tripleList.size(); i++) {
			int counter = 0;
			ArrayList<SeedingIndexes> plansTemp = new ArrayList<SeedingIndexes>(plans); // creates a copy of the plans List, that is changeble for each iteration.
			ArrayList<ExecutionStep> execution = new ArrayList<ExecutionStep>();
			boolean finished = false;
			// Merges The seeding Indexes until a final plan is generated.
			while (!finished) {
				// The triples that are merged with the starting triple. 
				search: {
				for (int j = 0; j < plans.size(); j++) {
					for (int k = 0; k < plans.get(i).getVariables().size(); k++) {
						for (int l = 0; l < plans.get(j).getVariables().size(); l++) {
						     if (plansTemp.get(i).getVariables().get(k) == plans.get(j).getVariables().get(l)
								&& plans.get(i) != plans.get(j) && plans.get(i).getSorted() == plans.get(i).getSorted()) {

							     
							     // Prepares the leftHand Step
							     ArrayList<TriplePattern2> patternLeft = new ArrayList<TriplePattern2>();
							     for (Integer iD : plans.get(i).getIDs()) {
									patternLeft.add(tripleList.get(iD));
								}
							     Integer indexType; 
							     if (i == 0) {
									indexType = plansTemp.get(i).getIndexesVariable1()[0];
								} else {
									indexType = plansTemp.get(i).getIndexesVariable2()[0];
								}
							     HandStep leftHand = new HandStep(patternLeft, indexType ,plansTemp.get(i).getIDs());
							     
							     //Prepares the right hand step:
							     ArrayList<TriplePattern2> patternRight = new ArrayList<TriplePattern2>();
							     for (Integer iD : plans.get(j).getIDs()) {
									patternRight.add(tripleList.get(iD));
								}
							     Integer indexTypeR; 
							     if (i == 0) {
									indexTypeR = plansTemp.get(j).getIndexesVariable1()[0];
								} else {
									indexTypeR = plansTemp.get(j).getIndexesVariable2()[0];
								}
							     HandStep rightHand = new HandStep(patternLeft, indexTypeR ,plansTemp.get(j).getIDs());
							     // ExecutionStep:
							     byte sort = 0;
							     byte joinType = 0; 
							     ExecutionStep exec = new ExecutionStep(joinType, leftHand, rightHand, sort);
							     execution.add(exec);
							     
							     // Joins i with j and because the triples are scanned, it sets the indexes to null,
							     // Set sorted to sorted by the used variable:
							     plansTemp.get(i).getIDs().add(j);
							     plansTemp.get(i).getVariables().addAll(plans.get(j).getVariables());
							     plansTemp.get(i).setIndexesVariable1(null);
							     plansTemp.get(i).setIndexesVariable2(null);
							     plansTemp.get(j).setSorted(plansTemp.get(i).getVariables().get(k));
							 
							     // Sets the elements of j to null to mark it as used for the join:
							     plansTemp.get(j).setIDs(null);
							     plansTemp.get(j).setIndexesVariable1(null);
							     plansTemp.get(j).setIndexesVariable2(null);
							     plansTemp.get(j).setVariables(null);
							     plansTemp.get(j).setSorted(null);
								 counter++;
							     break search;

					          }
						     else if(plansTemp.get(i).getVariables().get(k) == plans.get(j).getVariables().get(l)
								&& plans.get(i) != plans.get(j)) {
						    	 // Basically the same as the if step, just with sort=1,
						    	 // ToDo bring it into a function to remove redundancy.
						    	// Prepares the leftHand Step
							     ArrayList<TriplePattern2> patternLeft = new ArrayList<TriplePattern2>();
							     for (Integer iD : plans.get(i).getIDs()) {
									patternLeft.add(tripleList.get(iD));
								}
							     Integer indexType; 
							     if (i == 0) {
									indexType = plansTemp.get(i).getIndexesVariable1()[0];
								} else {
									indexType = plansTemp.get(i).getIndexesVariable2()[0];
								}
							     HandStep leftHand = new HandStep(patternLeft, indexType ,plansTemp.get(i).getIDs());
							     
							     //Prepares the right hand step:
							     ArrayList<TriplePattern2> patternRight = new ArrayList<TriplePattern2>();
							     for (Integer iD : plans.get(j).getIDs()) {
									patternRight.add(tripleList.get(iD));
								}
							     Integer indexTypeR; 
							     if (i == 0) {
									indexTypeR = plansTemp.get(j).getIndexesVariable1()[0];
								} else {
									indexTypeR = plansTemp.get(j).getIndexesVariable2()[0];
								}
							     HandStep rightHand = new HandStep(patternLeft, indexTypeR ,plansTemp.get(j).getIDs());
							     // ExecutionStep:
							     byte sort = 1;
							     byte joinType = 0; 
							     ExecutionStep exec = new ExecutionStep(joinType, leftHand, rightHand, sort);
							     execution.add(exec);
							     
							     // Joins i with j and because the triples are scanned, it sets the indexes to null,
							     // Set sorted to sorted by the used variable:
							     plansTemp.get(i).getIDs().add(j);
							     plansTemp.get(i).getVariables().addAll(plans.get(j).getVariables());
							     plansTemp.get(i).setIndexesVariable1(null);
							     plansTemp.get(i).setIndexesVariable2(null);
							     plansTemp.get(j).setSorted(plansTemp.get(i).getVariables().get(k));
							 
							     // Sets the elements of j to null to mark it as used for the join:
							     plansTemp.get(j).setIDs(null);
							     plansTemp.get(j).setIndexesVariable1(null);
							     plansTemp.get(j).setIndexesVariable2(null);
							     plansTemp.get(j).setVariables(null);
							     plansTemp.get(j).setSorted(null);
								 counter++;
							     break search;
						     }
				        }
					}
				}
			}
			if(counter ==  plans.size() - 1) {
				allMergePlansExecutionSteps.add(execution);
				finished = true;
			}
			}
		}

	}



	public void generateAllHashPlans() {
		// plans to evaluate only one starting pattern, then hash join with the rests.

	}

	public ArrayList<Byte> getPossibleIndexes(TriplePattern2 pattern) {
		return null;
	}

}
