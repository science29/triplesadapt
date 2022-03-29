package optimizer.planner;

import QueryStuff.Query;
import triple.TriplePattern;

import java.util.ArrayList;

public class Plan {

	private Query query; // Get the triples list
	private ArrayList<ExecutionStep> steps; // Join Types needed
	private ArrayList<TriplePattern> tripplePatterns; // List of triples how to get it?
	private ArrayList<SeedingIndexes> tripleIndexes = new ArrayList<SeedingIndexes>();
	private int usedIndexes[];

	public class SeedingIndexes {
		/**
		 * Inner class that gives the ArrayList tripleIndexes the values of the possible
		 * Indexes and used variables.
		 */

		private Integer tripleIndex1;
		private Integer tripleIndex2;
		private Integer variable1;
		private Integer variable2;
		public Integer getTripleIndex1() {
			return tripleIndex1;
		}
		public void setTripleIndex1(Integer tripleIndex1) {
			this.tripleIndex1 = tripleIndex1;
		}
		public Integer getTripleIndex2() {
			return tripleIndex2;
		}
		public void setTripleIndex2(Integer tripleIndex2) {
			this.tripleIndex2 = tripleIndex2;
		}
		public Integer getVariable1() {
			return variable1;
		}
		public void setVariable1(Integer variable1) {
			this.variable1 = variable1;
		}
		public Integer getVariable2() {
			return variable2;
		}
		public void setVariable2(Integer variable2) {
			this.variable2 = variable2;
		}



	}
	// Chooses the possible Indexes and the variables for the needed Dynamic Programming.
	public ArrayList<SeedingIndexes> SeedingDPTable() {
		for (TriplePattern triple : tripplePatterns) {
			// The first two fields are seeded with the usable Indexes, the last two fields
			// with the Variables;
			// For the Index the Indexpool was used.
			SeedingIndexes tripleIndexElement = new SeedingIndexes();
			if (triple.triples[0] > 0 && triple.triples[1] > 0) {
				tripleIndexElement.setTripleIndex1(2);
				tripleIndexElement.setTripleIndex2(6);
				tripleIndexElement.setVariable1(triple.triples[2]);
				tripleIndexElement.setVariable2(null); // dummy value, that no variable exists.
			} else if (triple.triples[0] > 0 && triple.triples[2] > 0) {
				tripleIndexElement.setTripleIndex1(4);
				tripleIndexElement.setTripleIndex2(10);
				tripleIndexElement.setVariable1(triple.triples[1]);
				tripleIndexElement.setVariable2(null);
			} else if (triple.triples[0] > 0) {
				tripleIndexElement.setTripleIndex1(2);
				tripleIndexElement.setTripleIndex2(4);
				tripleIndexElement.setVariable1(triple.triples[1]);
				tripleIndexElement.setVariable2(triple.triples[2]);
			} else if (triple.triples[1] > 0 && triple.triples[2] > 0) {
				tripleIndexElement.setTripleIndex1(8);
				tripleIndexElement.setTripleIndex2(12);
				tripleIndexElement.setVariable1(triple.triples[0]);
				tripleIndexElement.setVariable2(null);
			} else if (triple.triples[1] > 0) {
				tripleIndexElement.setTripleIndex1(6);
				tripleIndexElement.setTripleIndex2(8);
				tripleIndexElement.setVariable1(triple.triples[0]);
				tripleIndexElement.setVariable2(triple.triples[2]);
			} else if (triple.triples[2] > 0) {
				tripleIndexElement.setTripleIndex1(10);
				tripleIndexElement.setTripleIndex2(12);
				tripleIndexElement.setVariable1(triple.triples[0]);
				tripleIndexElement.setVariable2(triple.triples[1]);
			}

		}

		return tripleIndexes;
	}
	// Prunes the usable Indexes. Indexes that are not build are deleted.
	public ArrayList<SeedingIndexes> Pruning() {
		tripleIndexes = SeedingDPTable();
		for (int i = 0; i < tripleIndexes.size(); i++) {
			boolean Index1InList = false;
			boolean Index2InList = false;
			for (int j = 0; j < usedIndexes.length; j++) {
				if (tripleIndexes.get(i).getTripleIndex1() == usedIndexes[j]) {
					Index1InList = true;
				} else if (tripleIndexes.get(i).getTripleIndex2() == usedIndexes[j]) {
					Index2InList = true;
				}
			}
			if(Index1InList == false) {
				tripleIndexes.get(i).setTripleIndex1(null);
			}
			if(Index2InList == false) {
				tripleIndexes.get(i).setVariable2(null);
			}
			if(tripleIndexes.get(i).getTripleIndex1() == null && tripleIndexes.get(i).getTripleIndex2() == null) {
				System.out.println("The query can not be solved, because indexes that are needed are missing."  );
				break;
			}
		}

		return tripleIndexes;
	}

}