package optimizer.planner;

import java.util.ArrayList;

public class SeedingIndexes {
	/**
	 * Class that gives the ArrayList tripleIndexes the values of the possible
	 * Indexes and used variables.
	 */

	private ArrayList<Integer> IDs;
	private Integer IndexesVariable1[];
	private Integer IndexesVariable2[];
	private ArrayList<Integer> variables;
	private Integer Sorted;


	public ArrayList<Integer> getIDs() {
		return IDs;
	}
	public void setIDs(ArrayList<Integer> iDs) {
		IDs = iDs;
	}
	public Integer[] getIndexesVariable1() {
		return IndexesVariable1;
	}
	public void setIndexesVariable1(Integer[] indexesVariable1) {
		IndexesVariable1 = indexesVariable1;
	}
	public Integer[] getIndexesVariable2() {
		return IndexesVariable2;
	}
	public void setIndexesVariable2(Integer[] indexesVariable2) {
		IndexesVariable2 = indexesVariable2;
	}
	public ArrayList<Integer> getVariables() {
		return variables;
	}
	public void setVariables(ArrayList<Integer> variables) {
		this.variables = variables;
	}
	public Integer getSorted() {
		return Sorted;
	}
	public void setSorted(Integer sorted) {
		Sorted = sorted;
	}

}
