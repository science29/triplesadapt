package optimizer.Rules;

import QueryStuff.Query;
import info.uniadapt.api.Assistance;
import info.uniadapt.api.DeepOptim;
import info.uniadapt.api.StatHandler;
import optimizer.SourceSelection;

import java.util.ArrayList;
import static info.uniadapt.api.StatHandler.getgeneralBorderAccessRate;

public class GeneralReplicationRule extends  GeneralRule {

    //public  double borderBenefit;

    public double benefitPerItem;
    public double stepSize;
    private final GeneralReplicationInfo generalReplicationInfo;

    public ArrayList<BorderOperationalRule> borderOperationalRules;
    public DeepOptim deepOptim ;


    public GeneralReplicationRule(byte indexType , GeneralReplicationInfo generalReplicationInfo ,SourceSelection sourceSelection , DeepOptim deepOptim) {
        super(indexType , sourceSelection);
        this.generalReplicationInfo = generalReplicationInfo;
        for(int i = 0; i < StatHandler.getMaxBorderDistance(); i++){
            borderOperationalRules.add( new BorderOperationalRule(i,getgeneralBorderAccessRate(),indexType));
        }
        this.deepOptim = deepOptim;
        deepOptim.setGeneralReplicationRule(this);
    }


    public double getBorderBenefit(int distance){
        if(distance > generalReplicationInfo.maxQueryLength)
            return 0;
        double benefit = ((generalReplicationInfo.averageQueryLength/2.0)/distance)
                * generalReplicationInfo.itemMoveOverNetworkCost * generalReplicationInfo.averageBorderReplicationUsage;
        return  benefit;
    }

    public OperationalRule.TripleBlock getTripleBlock(){
       return deepOptim.getNextTriplesBlock();
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
