package optimizer.planner;

import java.util.ArrayList;

public class SeedingIndexes implements Cloneable{
	/**
	 * Class that gives the ArrayList tripleIndexes the values of the possible
	 * Indexes and used variables.
	 */

	private ArrayList<Integer> IDs;
	private Integer IndexesVariable1[];
	private Integer IndexesVariable2[];
	private ArrayList<Integer> variables;
	private ArrayList<Integer> sorted;

	
	public SeedingIndexes(ArrayList<Integer> iDs, Integer[] indexesVariable1, Integer[] indexesVariable2,
			ArrayList<Integer> variables, ArrayList<Integer> sorted) {
		super();
		this.IDs = iDs;
		this.IndexesVariable1 = indexesVariable1;
		this.IndexesVariable2 = indexesVariable2;
		this.variables = variables;
		this.sorted = sorted;
	}

	public SeedingIndexes() {
		
	}
	

	@Override
	protected SeedingIndexes clone() throws CloneNotSupportedException {
		
		SeedingIndexes clonedSeedingIndexes = null;
		try {
			   clonedSeedingIndexes = (SeedingIndexes) super.clone();
	           clonedSeedingIndexes.setIDs((ArrayList<Integer>)this.IDs.clone());
	           clonedSeedingIndexes.setIndexesVariable1((Integer [])this.IndexesVariable1.clone()); 
	           clonedSeedingIndexes.setIndexesVariable2((Integer [])this.IndexesVariable2.clone()); 
	           clonedSeedingIndexes.setVariables((ArrayList<Integer>)this.variables.clone());
	        //   clonedSeedingIndexes.setSorted((Integer[])this.Sorted.clone());;
		} catch (CloneNotSupportedException e) {
            e.printStackTrace();
		}

		return clonedSeedingIndexes;
	}


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
	public ArrayList<Integer> getSorted() {
		return sorted;
	}
	public void setSorted(ArrayList<Integer> sorted) {
		this.sorted = sorted;
	}
}