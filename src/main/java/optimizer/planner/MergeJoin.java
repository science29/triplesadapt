package optimizer.planner;

import java.util.ArrayList;
import java.util.Iterator;

import triple.TriplePattern2;

public class MergeJoin {

	ArrayList<TriplePattern2> tripleList;
	ArrayList<ExecutionStep> execution;

	public ArrayList<SeedingIndexes> MergePlans(int run, ArrayList<SeedingIndexes> merge,
			ArrayList<TriplePattern2> tripleList, ArrayList<ArrayList<ExecutionStep>> execution,
			ArrayList<ArrayList<SeedingIndexes>> MergeTemp) throws CloneNotSupportedException {

		if (!MergeTemp.isEmpty()) {
			int listSizeBeforeLoop = MergeTemp.size();
			int executionSize = execution.size();
			System.out.println("current size execution: " + executionSize);
			if (MergeTemp.get(0).size() == 2) { // special case if there are only two triple.
				return merge;
			}
			for (int p = 0; p < listSizeBeforeLoop; p++) {
				loopingPlans(p, run, MergeTemp.get(p), tripleList, MergeTemp, execution);
			}
			int count = 0;
			while (count < listSizeBeforeLoop) {
				MergeTemp.remove(0);
				count++;
			}
			int count1 = 0;
			while (count1 < executionSize) {
				execution.remove(0);
				count1++;
			}

			if (MergeTemp.get(0).get(0).getIDs().size() < MergeTemp.get(0).size()) {
				return MergePlans(run, merge, tripleList, execution, MergeTemp);
			}
		} else {
			loopingPlans(0, run, merge, tripleList, MergeTemp, execution);
			System.out.println("Execution Step size: " + execution.size());
			run++;
			return MergePlans(run, merge, tripleList, execution, MergeTemp);
		}

		return merge;
	}

	public static void loopingPlans(int p, int run, ArrayList<SeedingIndexes> merge,
			ArrayList<TriplePattern2> tripleList, ArrayList<ArrayList<SeedingIndexes>> MergeTemp,
			ArrayList<ArrayList<ExecutionStep>> execution) throws CloneNotSupportedException {
		for (int i = 0; i < merge.size(); i++) {
			if (merge.get(i).getIDs().get(0) != -1) {
				for (int j = 0; j < merge.size(); j++) {
					if (merge.get(j).getIDs().get(0) != -1) {
						boolean joinConflict = false; // test if i and j contain duplicate elements.
						for (int id = 0; id < merge.get(i).getIDs().size(); id++) {
							if (merge.get(j).getIDs().contains(merge.get(i).getIDs().get(id))) {
								joinConflict = true;
							}
						}
						if (i != j && !joinConflict) {
							ArrayList<Integer> intersection = new ArrayList<Integer>();
							intersection.addAll(merge.get(i).getVariables());
							intersection.retainAll(merge.get(j).getVariables());
							if (!intersection.isEmpty()) {
								/**
								 * Prepares the left hand step: Adds also sorted for the later join.
								 */
								ArrayList<TriplePattern2> patternLeft = new ArrayList<TriplePattern2>();
								for (int k = 0; k < merge.get(i).getIDs().size(); k++) {
									if (merge.get(i).getIDs().get(k) == null) {
										break;
									} else {
										patternLeft.add(tripleList.get(merge.get(i).getIDs().get(k)));
									}
								}
								Integer indexType = 0;
								Integer sorted = 1;
								int sortLeft = 1; // if the left step has to be sorted.
								int sortRight = 1; // if the right step has to be sorted.
								byte sort; // pattern need to be sorted (1) or not (0).
								if (merge.get(i).getIDs().size() < 2) { // i's first merge:
									if (merge.get(j).getIDs().size() < 2) {
										// j's first merge: test which variable
										// equals the first result of the intersection and add the according index.
										if (merge.get(i).getVariables().size() == 1) { // just one variable exists
											indexType = merge.get(i).getIndexesVariable1()[0];
											sorted = intersection.get(0);
											sortLeft = 0;
										} else if (intersection.contains(merge.get(i).getVariables().get(0))
												&& merge.get(i).getIndexesVariable1()[0] != -1) {
											indexType = merge.get(i).getIndexesVariable1()[0];
											sorted = merge.get(i).getVariables().get(0);
											sortLeft = 0;
										} else if (intersection.contains(merge.get(i).getVariables().get(1))
												&& merge.get(i).getIndexesVariable2()[0] != -1) {
											indexType = merge.get(i).getIndexesVariable2()[0];
											sorted = merge.get(i).getVariables().get(1);
											sortLeft = 0;
										} else if (intersection.contains(merge.get(i).getVariables().get(0))) {
											// Variable 1 is a match, but the according index does not exist so Variable
											// 2 index has to be used. Therefore sorting is necessary.
											indexType = merge.get(i).getIndexesVariable2()[0];
											sorted = merge.get(i).getVariables().get(0);
											sortLeft = 1;
										} else {
											indexType = merge.get(i).getIndexesVariable1()[0];
											sorted = merge.get(i).getVariables().get(1);
											sortLeft = 1;
										}
									} else { // j was already merged and has a fixed index scan, attempt to match
												// i's index scan so that no sort operation is necessary.
										if (merge.get(i).getVariables().contains(merge.get(j).getSorted().get(0))) {
											if (merge.get(j).getSorted().get(0)
													.equals(merge.get(i).getVariables().get(0))) {
												if (merge.get(i).getIndexesVariable1()[0] != -1) {
													indexType = merge.get(i).getIndexesVariable1()[0];
													sorted = merge.get(j).getSorted().get(0);
													sortLeft = 0;
												} else {
													indexType = merge.get(i).getIndexesVariable2()[0];
													sorted = merge.get(j).getSorted().get(0);
													sortLeft = 1;
												}
											} else if (merge.get(j).getSorted().get(0)
													.equals(merge.get(i).getVariables().get(1))) {
												if (merge.get(i).getIndexesVariable2()[0] != -1) {
													indexType = merge.get(i).getIndexesVariable2()[0];
													sorted = merge.get(j).getSorted().get(0);
													sortLeft = 0;
												} else {
													indexType = merge.get(i).getIndexesVariable1()[0];
													sorted = merge.get(j).getSorted().get(0);
													sortLeft = 1;
												}
											}
										} else {
											if (merge.get(j).getVariables().contains(merge.get(i).getVariables().get(0))
													&& merge.get(i).getIndexesVariable1()[0] != -1) {
												indexType = merge.get(i).getIndexesVariable1()[0];
												sorted = merge.get(i).getVariables().get(0);
												sortLeft = 0;
											} else if (merge.get(j).getVariables()
													.contains(merge.get(i).getVariables().get(1))
													&& merge.get(i).getIndexesVariable2()[0] != -1) {
												indexType = merge.get(i).getIndexesVariable2()[0];
												sorted = merge.get(i).getVariables().get(1);
												sortLeft = 0;
											} else if (merge.get(j).getVariables()
													.contains(merge.get(i).getVariables().get(0))) {
												indexType = merge.get(i).getIndexesVariable2()[0];
												sorted = merge.get(i).getVariables().get(0);
												sortLeft = 1;
											} else {
												indexType = merge.get(i).getIndexesVariable1()[0];
												sorted = merge.get(i).getVariables().get(1);
												sortLeft = 1;
											}

										}
									}

								} else { // i already merged index scan was already used and can not be changed.
									indexType = merge.get(i).getIndexesVariable1()[0];
									if (merge.get(j).getSorted().contains(merge.get(i).getSorted().get(0))) {
										sorted = merge.get(i).getSorted().get(0);
										sortLeft = 0;
									} else if (intersection.contains(merge.get(i).getSorted().get(0))) {
										sorted = merge.get(i).getSorted().get(0);
										sortLeft = 0;
										sortRight = 1;
									} else if (intersection.contains(merge.get(j).getSorted().get(0))) {
										sorted = merge.get(j).getSorted().get(0);
										sortLeft = 1;
									} else {
										sorted = intersection.get(0);
										sortLeft = 1;
									}
								}
								HandStep leftHand = new HandStep(patternLeft, indexType, merge.get(i).getIDs());

								/**
								 * Prepares the right hand step:
								 */
								ArrayList<TriplePattern2> patternRight = new ArrayList<TriplePattern2>();
								for (int l = 0; l < merge.get(j).getIDs().size(); l++) {
									if (merge.get(j).getIDs().get(l) == null) {
										break;
									} else {
										patternRight.add(tripleList.get(merge.get(j).getIDs().get(l)));
									}
								}
								Integer indexTypeR = 0;
								if (merge.get(j).getIDs().size() < 2) { // j's first merge:
									if (merge.get(i).getIDs().size() < 2) { // i's first merge: test which
										// variable equals the first result of the intersection and add the according
										// index.
										if (merge.get(j).getVariables().size() == 1) { // just one variable exists
											indexTypeR = merge.get(j).getIndexesVariable1()[0];
											sortRight = 0;
										} else if (intersection.contains(merge.get(j).getVariables().get(0))
												&& merge.get(j).getIndexesVariable1()[0] != -1) {
											indexTypeR = merge.get(j).getIndexesVariable1()[0];
											sortRight = 0;
										} else if (intersection.contains(merge.get(j).getVariables().get(1))
												&& merge.get(j).getIndexesVariable2()[0] != -1) {
											indexTypeR = merge.get(j).getIndexesVariable2()[0];
											sortRight = 0;
										} else if (intersection.contains(merge.get(j).getVariables().get(0))) {
											// Variable 1 is a match, but the according index does not exist so Variable
											// 2
											// index has to be used.
											indexTypeR = merge.get(j).getIndexesVariable2()[0];
											sortRight = 1;
										} else {
											indexTypeR = merge.get(j).getIndexesVariable1()[0];
											sortRight = 1;
										}

									} else { // i was already merged and has a fixed index scan, attempt to match
												// i's index scan so that no sort operation is necessary.
										if (merge.get(j).getVariables().contains(merge.get(i).getSorted().get(0))) {
											if (merge.get(i).getSorted().get(0)
													.equals(merge.get(j).getVariables().get(0))) {
												if (merge.get(j).getIndexesVariable1()[0] != -1) {
													indexTypeR = merge.get(j).getIndexesVariable1()[0];
													sortRight = 0;
												} else {
													indexTypeR = merge.get(j).getIndexesVariable2()[0];
													sortRight = 1;
												}
											} else if (merge.get(i).getSorted().get(0)
													.equals(merge.get(j).getVariables().get(1))) {
												if (merge.get(j).getIndexesVariable2()[0] != -1) {
													indexTypeR = merge.get(j).getIndexesVariable2()[0];
													sortRight = 0;
												} else {
													indexTypeR = merge.get(j).getIndexesVariable1()[0];
													sortLeft = 1;
												}
											}
										} else {
											if (merge.get(i).getVariables().contains(merge.get(j).getVariables().get(0))
													&& merge.get(j).getIndexesVariable1()[0] != -1) {
												indexTypeR = merge.get(j).getIndexesVariable1()[0];
												sortRight = 0;
											} else if (merge.get(i).getVariables()
													.contains(merge.get(j).getVariables().get(1))
													&& merge.get(j).getIndexesVariable2()[0] != -1) {
												indexTypeR = merge.get(j).getIndexesVariable2()[0];
												sortRight = 0;
											}  else if (merge.get(i).getVariables()
													.contains(merge.get(j).getVariables().get(0))) {
												indexTypeR = merge.get(j).getIndexesVariable2()[0];
												sortRight = 1;
											} else {
												indexTypeR = merge.get(j).getIndexesVariable1()[0];
												sortRight = 1;
											}

										}
									}

								} else { // j already merged index scan was already used and can not be changed.
									indexTypeR = merge.get(j).getIndexesVariable1()[0];
									if (merge.get(i).getSorted().contains(merge.get(j).getSorted().get(0))) {
										sortRight = 0;
									} else if (intersection.contains(merge.get(j).getSorted().get(0))) {
										sortLeft = 1;
										sortRight = 0;
									} else if (intersection.contains(merge.get(i).getSorted().get(0))) {
										sortRight = 1;
									} else {
										sortRight = 1;
									}
								}
								HandStep rightHand = new HandStep(patternRight, indexTypeR, merge.get(j).getIDs());

								/**
								 * Execution Step:
								 */
								sort = 1;
								if (sortLeft == 0 && sortRight == 0) {
									sort = 0;
								}

								byte joinType = 0;
								ExecutionStep exec = new ExecutionStep(joinType, leftHand, rightHand, sort);
								ArrayList<ExecutionStep> executionPlan = new ArrayList<ExecutionStep>();
								if (run > 0) { // in the first run the execution List is build in further runs the steps
									// before are used to build an execution plan.
									executionPlan.addAll(execution.get(p));
								}
								executionPlan.add(exec);
								execution.add(executionPlan);

								/**
								 * Joining i and j in element i and deleting j after that.
								 */
								// IDs merging:
								ArrayList<SeedingIndexes> newMerge = new ArrayList<SeedingIndexes>();
								Iterator<SeedingIndexes> iterator = merge.iterator();
								while (iterator.hasNext()) {
									newMerge.add((SeedingIndexes) iterator.next().clone());
								}

								newMerge.get(i).getIDs().addAll(newMerge.get(j).getIDs());
								// IndexVariable1 is set to the used index in the merge.
								Integer[] indexI = { indexType };
								newMerge.get(i).setIndexesVariable1(indexI);
								// Variables merging:
								newMerge.get(j).getVariables().removeAll(newMerge.get(i).getVariables());
								newMerge.get(i).getVariables().addAll(newMerge.get(j).getVariables());
								// setting sorted new:
								ArrayList<Integer> newSorted = new ArrayList<Integer>();
								newSorted.add(sorted);
								newMerge.get(i).setSorted(newSorted);
								// Deleting j:
								ArrayList<Integer> newIdj = new ArrayList<Integer>();
								newIdj.add(-1); // dummy Value that j is used.
								newMerge.get(j).setIDs(newIdj);
								MergeTemp.add(newMerge);
							}
						}
					}
				}
			}
		}
	}
}