package optimizer.Rules;

import optimizer.Replication.ReplicationStep;

public class GeneralReplicationInfo {

    public double averageBorderReplicationUsage = 1;
    public double averageQueryLength;
    public double averageBorderLenght;
    public double maxQueryLength;
    public double itemMoveOverNetworkCost;

    private int total = 0;

    public ReplicationStep currentStep;


    public GeneralReplicationInfo(double itemMoveOverNetworkCost){
        this.itemMoveOverNetworkCost = itemMoveOverNetworkCost;
    }

    public void inform(int replicationLength , int queryLenght){
        total++;
        this.averageBorderLenght = (averageBorderLenght + replicationLength) / total;
        this.averageQueryLength = (averageQueryLength + queryLenght) / total;
        if(maxQueryLength < queryLenght)
            maxQueryLength = queryLenght;
    }
}
