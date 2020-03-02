package optimizer.Rules;

import QueryStuff.Query;
import optimizer.SourceSelection;

public class GeneralReplicationRule extends  GeneralRule {

    //public  double borderBenefit;

    public double benefitPerItem;
    public double stepSize;
    private final GeneralReplicationInfo generalReplicationInfo;

    public GeneralReplicationRule(byte indexType , GeneralReplicationInfo generalReplicationInfo ,SourceSelection sourceSelection) {
        super(indexType , sourceSelection);
        this.generalReplicationInfo = generalReplicationInfo;
    }


    public double getBorderBenefit(int distance){
        if(distance > generalReplicationInfo.maxQueryLength)
            return 0;
        double benefit = ((generalReplicationInfo.averageQueryLength/2.0)/distance)
                * generalReplicationInfo.itemMoveOverNetworkCost * generalReplicationInfo.averageBorderReplicationUsage;
        return  benefit;
    }


    @Override
    public void calculateBenefit() {

        calculateNextStepBenefit();


    }

    private void calculateNextStepBenefit() {
        benefitPerItem = getBorderBenefit(generalReplicationInfo.currentStep.distance);
        //stepSize = generalReplicationInfo.getNextStepSize();
        generalBenefit = benefitPerItem;
    }


}
