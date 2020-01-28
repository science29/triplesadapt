package optimizer.Rules;

import QueryStuff.Query;

public abstract class Rule {
    public final byte indexType;
    public long occupation = 0;
    public int usage  = 0 ;
    public Query scope;
    public Rule(byte indexType , Query scope) {
        this.indexType = indexType;
        this.scope = scope;
    }

    public abstract void calculateBenefit();



}
