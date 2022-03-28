package optimizer.Rules;

import optimizer.SourceSelection;

public class GeneralRule extends Rule{

    private final XRuleXOpHandler xRuleXOpHandler;
    public int expectedSize;

    public void setGeneralAbsoulteBenefit(double generalAbsoulteBenefit) {
        this.generalAbsoulteBenefit = generalAbsoulteBenefit;
    }

    double generalBenefit = 0 ;

    double relativeUsage;



    double generalAbsoulteBenefit;


    boolean baseLineIndex = false;

    public double getGeneralBenefit(){
        return generalBenefit;
    }

    public GeneralRule(byte indexType , SourceSelection sourceSelection)  {
        super(indexType, sourceSelection);
        xRuleXOpHandler = new XRuleXOpHandler(indexType);
    }


    public OperationalRule.TripleBlock getNextBlock(){
        return xRuleXOpHandler.getNextTriplesBlock();
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
