package optimizer.Rules;

import QueryStuff.Query;
import optimizer.SourceSelection;

public abstract class Rule {
    public final byte indexType;
    public long occupation = 0;
    public int usage  = 0 ;
    public SourceSelection scope;
    public Rule(byte indexType , SourceSelection scope) {
        this.indexType = indexType;
        this.scope = scope;
    }

    public abstract void calculateBenefit();



}
