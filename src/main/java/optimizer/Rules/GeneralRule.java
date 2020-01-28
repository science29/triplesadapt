package optimizer.Rules;

import QueryStuff.Query;

public class GeneralRule extends Rule{

    double generalBenefit = 0 ;


    public double getGeneralBenefit(){
        return generalBenefit;
    }

    public GeneralRule(byte indexType)  {
        super(indexType, null);
    }


    @Override
    public void calculateBenefit() {
        generalBenefit = usage;
    }





}
