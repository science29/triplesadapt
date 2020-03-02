package optimizer.Rules;

import QueryStuff.Query;
import optimizer.SourceSelection;

public class GeneralRule extends Rule{

    double generalBenefit = 0 ;
    double relativeUsage;
    double generalAbsoulteBenefit;

    public double getGeneralBenefit(){
        return generalBenefit;
    }

    public GeneralRule(byte indexType , SourceSelection sourceSelection)  {
        super(indexType, sourceSelection);
    }


    @Override
    public void calculateBenefit() {
        generalBenefit = relativeUsage * generalAbsoulteBenefit;
    }



    public void calculateGeneralBenefit(double generalAbsoulteBenefit) {

        this.generalAbsoulteBenefit = generalAbsoulteBenefit;
        generalBenefit = relativeUsage * generalAbsoulteBenefit;
    }



}
