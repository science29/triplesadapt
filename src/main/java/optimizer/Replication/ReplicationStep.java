package optimizer.Replication;

public class ReplicationStep {

    public int nodeNumber;
    public int distance;
    public int tripleCountWithinNode;
    public int tripleCountWithinDistance;

    public ReplicationStep(int nodeNumber, int distance, int tripleCountWithinNode) {
        this.nodeNumber = nodeNumber;
        this.distance = distance;
        this.tripleCountWithinNode = tripleCountWithinNode;
    }
}
