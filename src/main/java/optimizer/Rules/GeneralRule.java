package optimizer.Rules;

import QueryStuff.Query;
import optimizer.SourceSelection;

public class GeneralRule extends Rule{

    double generalBenefit = 0 ;

    double relativeUsage;



    double generalAbsoulteBenefit;


    boolean baseLineIndex = false;

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


    public boolean isBaseLineIndex() {
        return baseLineIndex;
    }
    public void setBaseLineIndex(boolean baseLineIndex) {
        this.baseLineIndex = baseLineIndex;
    }


    public void setRelativeUsage(double relativeUsage) {
        this.relativeUsage = relativeUsage;
    }


}
