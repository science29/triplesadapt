package optimizer.planner;

import java.util.ArrayList;
import java.util.Iterator;

import triple.TriplePattern2;

public class MergeJoin {

	ArrayList<TriplePattern2> tripleList;
	ArrayList<ExecutionStep> execution;

	public ArrayList<SeedingIndexes> MergePlans(ArrayList<SeedingIndexes> merge, ArrayList<TriplePattern2> tripleList,
			ArrayList<ExecutionStep> execution, ArrayList<ArrayList<SeedingIndexes>> MergeTemp)
			throws CloneNotSupportedException {
		/*
		 * ToDo: Make it that after the starting loop the MergeTemp list is filled with
		 * steps, so that every possible step starting by i is saved
		 */
		if (!MergeTemp.isEmpty()) {
			int listSizeBeforeLoop = MergeTemp.size();
			for (int i = 0; i < listSizeBeforeLoop; i++) {
				loopingPlans(MergeTemp.get(i), tripleList, MergeTemp, execution);
			}
			System.out.println(MergeTemp.size());
			if (MergeTemp.get(0).get(0).getIDs().size() < MergeTemp.get(0).size()) {
				return MergePlans(merge, tripleList, execution, MergeTemp);
			}
			int count = 0; // deletes the old plans.
			while (count < listSizeBeforeLoop) {
				MergeTemp.remove(0);
				count ++;
			}
		} else {
			loopingPlans(merge, tripleList, MergeTemp, execution);
			return MergePlans(merge, tripleList, execution, MergeTemp);
		}

		return merge;
	}

	public static void loopingPlans(ArrayList<SeedingIndexes> merge, ArrayList<TriplePattern2> tripleList,
			ArrayList<ArrayList<SeedingIndexes>> MergeTemp, ArrayList<ExecutionStep> execution)
			throws CloneNotSupportedException {
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
								Integer sorted = 0;
								if (merge.get(i).getVariables().size() < 3) { // i's first merge:
									if (merge.get(j).getVariables().size() < 3) {
										// j's first merge: test which variable
										// equals the first result of the intersection and add the according index.
										if (intersection.get(0).equals(merge.get(i).getVariables().get(0))) {
											indexType = merge.get(i).getIndexesVariable1()[0];
											sorted = intersection.get(0);
										} else if (intersection.get(0).equals(merge.get(i).getVariables().get(1))
												&& merge.get(i).getIndexesVariable1().length > 1) {
											indexType = merge.get(i).getIndexesVariable1()[1];
											sorted = intersection.get(0);
										} else if (intersection.get(0).equals(merge.get(i).getVariables().get(1))) {
											indexType = merge.get(i).getIndexesVariable2()[0];
											sorted = intersection.get(0);
										} else { // j was already merged and has a fixed index scan, attempt to match
													// i's
													// index
													// scan so that no sort operation is necessary.
											if (merge.get(i).getVariables().contains(merge.get(j).getSorted().get(0))) {
												if (merge.get(j).getSorted().get(0)
														.equals(merge.get(i).getVariables().get(0))) {
													indexType = merge.get(i).getIndexesVariable1()[0];
													sorted = merge.get(j).getSorted().get(0);
												} else if (merge.get(j).getSorted().get(0)
														.equals(merge.get(i).getVariables().get(1))) {
													if (merge.get(i).getIndexesVariable1().length > 1) {
														indexType = merge.get(i).getIndexesVariable1()[1];
														sorted = merge.get(j).getSorted().get(0);
													} else {
														indexType = merge.get(i).getIndexesVariable2()[0];
														sorted = merge.get(j).getSorted().get(0);
													}
												}
											}
										}
									}

								} else { // i already merged index scan was already used and can not be changed.
									indexType = merge.get(i).getIndexesVariable1()[0];
									if (merge.get(i).getSorted().get(0).equals(merge.get(j).getSorted().get(0))) {
										sorted = merge.get(j).getSorted().get(0);
									} else if (intersection.contains(merge.get(i).getSorted().get(0))) {
										sorted = merge.get(i).getSorted().get(0);
									} else if (intersection.contains(merge.get(j).getSorted().get(0))) {
										sorted = merge.get(j).getSorted().get(0);
									} else {
										sorted = intersection.get(0);
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
										patternLeft.add(tripleList.get(merge.get(j).getIDs().get(l)));
									}
								}
								Integer indexTypeR = 0;
								if (merge.get(j).getVariables().size() < 3) { // j's first merge:
									if (merge.get(i).getVariables().size() < 3) { // i's first merge: test which
																					// variable
										// equals the first result of the intersection and add the according index.
										if (intersection.get(0).equals(merge.get(j).getVariables().get(0))) {
											indexTypeR = merge.get(j).getIndexesVariable1()[0];
										} else if (intersection.get(0).equals(merge.get(j).getVariables().get(1))
												&& merge.get(j).getIndexesVariable1().length > 1) {
											indexTypeR = merge.get(j).getIndexesVariable1()[1];
										} else if (intersection.get(0).equals(merge.get(j).getVariables().get(1))) {
											indexTypeR = merge.get(j).getIndexesVariable2()[0];
										} else { // i was already merged and has a fixed index scan, attempt to match
													// i's
													// index
													// scan so that no sort operation is necessary.
											if (merge.get(j).getVariables().contains(merge.get(i).getSorted().get(0))) {
												if (merge.get(i).getSorted().get(0)
														.equals(merge.get(j).getVariables().get(0))) {
													indexTypeR = merge.get(j).getIndexesVariable1()[0];
												} else if (merge.get(i).getSorted().get(0)
														.equals(merge.get(j).getVariables().get(1))) {
													if (merge.get(j).getIndexesVariable1().length > 1) {
														indexTypeR = merge.get(j).getIndexesVariable1()[1];
													} else {
														indexTypeR = merge.get(j).getIndexesVariable2()[0];
													}
												}
											}
										}
									}

								} else { // i already merged index scan was already used and can not be changed.
									indexType = merge.get(j).getIndexesVariable1()[0];
								}
								HandStep rightHand = new HandStep(patternRight, indexTypeR, merge.get(j).getIDs());

								/**
								 * Execution Step:
								 */
								byte sort; // pattern need to be sorted (1) or not (0).
								if (merge.get(i).getSorted().size() == 2) { // i's first merge.
									if (merge.get(j).getSorted().size() == 2) { // j's first merge --> one sorted
																				// variable
																				// of each pattern has to match.
										sort = 0;
									} else if (merge.get(i).getSorted().contains(merge.get(j).getSorted().get(0))) {
										sort = 0;
									} else {
										sort = 1;
									}
								} else {
									if (merge.get(j).getSorted().size() == 2) {
										if (merge.get(j).getSorted().contains(merge.get(i).getSorted().get(0))) {
											sort = 0;
										} else {
											sort = 1;
										}
									} else if (merge.get(j).getSorted().get(0) == merge.get(i).getSorted().get(0)) {
										sort = 0;
									} else {
										sort = 1;
									}

								}
								byte joinType = 0;
								ExecutionStep exec = new ExecutionStep(joinType, leftHand, rightHand, sort);
								execution.add(exec);

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
								// Starting recursion:
							}
						}
					}
				}
			}
		}
	}
}