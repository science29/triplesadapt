package optimizer.planner;

public class ExecutionStep {

	protected HandStep leftHand;
	protected HandStep rightHand;
	protected byte joinType; // set to 0 for MergeJoin, otherwise to 1 for HashJoin.
	private int cost;
	private byte sort; // if it needs to be sorted 1, otherwise 0.

	public ExecutionStep(byte joinType, HandStep leftHand, HandStep rightHand, byte sort) {
		this.joinType = joinType;
		this.leftHand = leftHand;
		this.rightHand = rightHand;
		this.sort = sort;

	}

	public HandStep getLeftHand() {
		return leftHand;
	}

	public void setLeftHand(HandStep leftHand) {
		this.leftHand = leftHand;
	}

	public HandStep getRightHand() {
		return rightHand;
	}

	public void setRightHand(HandStep rightHand) {
		this.rightHand = rightHand;
	}

	public byte getJoinType() {
		return joinType;
	}

	public void setJoinType(byte joinType) {
		this.joinType = joinType;
	}

	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public byte getSort() {
		return sort;
	}

	public void setSort(byte sort) {
		this.sort = sort;
	}
}
