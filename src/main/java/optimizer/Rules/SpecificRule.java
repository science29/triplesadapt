package optimizer.Rules;

import QueryStuff.Query;
import optimizer.SourceSelection;

public class SpecificRule extends Rule{

    public final int first;
    public final int second;
    public int cost;

    public SpecificRule(byte indexType, int first , int second , SourceSelection scope) {
        super(indexType,scope);
        this.first = first;
        this.second = second;
    }

    @Override
    public void calculateBenefit() {

    }
}
