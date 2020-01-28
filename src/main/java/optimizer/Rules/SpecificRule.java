package optimizer.Rules;

import QueryStuff.Query;

public class SpecificRule extends Rule{

    public final int first;
    public final int second;
    public int cost;

    public SpecificRule(byte indexType, int first , int second , Query scope) {
        super(indexType,scope);
        this.first = first;
        this.second = second;
    }

    @Override
    public void calculateBenefit() {

    }
}
