package optimizer.planner;

import java.util.ArrayList;

import QueryStuff.Query;
import triple.TriplePattern2;

public class Plan {

	private Query query; // Get the triples list
	private ArrayList<ExecutionStep> steps; // Join Types needed
	private Integer usedIndexes[];
	private SeedingIndexes tripleIndexElement;

	// Chooses the possible Indexes and the variables for the needed Dynamic
	// Programming. As well as an ID to identify the Pattern, and set Sorted to null == false.
	public SeedingIndexes SeedingDPTable(Integer i, TriplePattern2 triple) {
		// The first two fields are seeded with the usable Indexes, the last two fields
		// with the Variables;
		// For the Index the Indexpool was used.
		tripleIndexElement = new SeedingIndexes();
		ArrayList<Integer> iDs = new ArrayList<Integer>();
		iDs.add(i); 
		tripleIndexElement.setIDs(iDs);
		tripleIndexElement.setSorted(null);
		ArrayList<Integer> variables = new ArrayList<Integer>();
		if (triple.getTriples()[0] > 0 && triple.getTriples()[1] > 0) {
			variables.add(Integer.valueOf(triple.getTriples()[2]));
			Integer[] indexesVariable1 = {2,6};
			Integer[] indexesVariable2 = {null};
			tripleIndexElement.setIndexesVariable1(indexesVariable1);
			tripleIndexElement.setIndexesVariable2(indexesVariable2);
			tripleIndexElement.setVariables(variables);; // dummy value, that no variable exists.
		} else if (triple.getTriples()[0] > 0 && triple.getTriples()[2] > 0) {
			Integer[] indexesVariable1 = {4, 10};
			Integer[] indexesVariable2 = {null};
			variables.add(Integer.valueOf(triple.getTriples()[1]));
			tripleIndexElement.setIndexesVariable1(indexesVariable1);
			tripleIndexElement.setIndexesVariable2(indexesVariable2);
			tripleIndexElement.setVariables(variables);
		} else if (triple.getTriples()[0] > 0) {
			Integer[] indexesVariable1 = {2};
			Integer[] indexesVariable2 = {4};
			variables.add(Integer.valueOf(triple.getTriples()[1]), Integer.valueOf(triple.getTriples()[2]));
			tripleIndexElement.setIndexesVariable1(indexesVariable1);
			tripleIndexElement.setIndexesVariable2(indexesVariable2);
			tripleIndexElement.setVariables(variables);;
		} else if (triple.getTriples()[1] > 0 && triple.getTriples()[2] > 0) {
			Integer[] indexesVariable1 = {8, 12};
			Integer[] indexesVariable2 = {null};
			variables.add(Integer.valueOf(triple.getTriples()[1]));
			variables.add(Integer.valueOf(triple.getTriples()[0]));
			tripleIndexElement.setIndexesVariable1(indexesVariable1);
			tripleIndexElement.setIndexesVariable2(indexesVariable2);
			tripleIndexElement.setVariables(variables);
		} else if (triple.getTriples()[1] > 0) {
			Integer[] indexesVariable1 = {6};
			Integer[] indexesVariable2 = {8};
			variables.add(Integer.valueOf(triple.getTriples()[0]), Integer.valueOf(triple.getTriples()[2]));
			tripleIndexElement.setIndexesVariable1(indexesVariable1);
			tripleIndexElement.setIndexesVariable2(indexesVariable2);
			tripleIndexElement.setVariables(variables);
		} else if (triple.getTriples()[2] > 0) {
			Integer[] indexesVariable1 = {10};
			Integer[] indexesVariable2 = {12};
			variables.add(Integer.valueOf(triple.getTriples()[0]), Integer.valueOf(triple.getTriples()[1]));
			tripleIndexElement.setIndexesVariable1(indexesVariable1);
			tripleIndexElement.setIndexesVariable2(indexesVariable2);
			tripleIndexElement.setVariables(variables);
		}

		return tripleIndexElement;
	}

	// Prunes the usable Indexes. Indexes that are not build are deleted.
	public SeedingIndexes Pruning(Integer i, TriplePattern2 triple) {
		tripleIndexElement = SeedingDPTable(i, triple);
		boolean Index10InList = false;
		boolean Index11InList = false;
		boolean Index2InList = false;
		for (int j = 0; j < usedIndexes.length; j++) {
			if (tripleIndexElement.getIndexesVariable1()[0] == usedIndexes[j]) {
				Index10InList = true;
			}else if (tripleIndexElement.getIndexesVariable1().length > 1) {
				if (tripleIndexElement.getIndexesVariable1()[1] == usedIndexes[j]) {
					Index11InList = true;
				}
			}
		else if (tripleIndexElement.getIndexesVariable2()[0] == usedIndexes[j]) {
				Index2InList = true;
			}
		}
		if (Index10InList == false) {
			tripleIndexElement.getIndexesVariable1()[0] = null;
		}
		if (Index11InList == false && tripleIndexElement.getIndexesVariable1().length > 1) {
			tripleIndexElement.getIndexesVariable1()[1] = null;
			}
		if (Index2InList == false) {
			tripleIndexElement.getIndexesVariable2()[0] = null;
		}
		if (Index10InList == false && Index11InList == false && Index2InList == false) {
			System.out.println("The query can not be solved, because indexes that are needed are missing.");
		}
		return tripleIndexElement;
	}

}