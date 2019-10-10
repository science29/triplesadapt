package optimizer;

public abstract class Rule {
    public final byte indexType;
    public int occupation = 0;
    public int usage  = 0 ;
    public Rule(byte indexType) {
        this.indexType = indexType;
    }



    public class GeneralRule extends Rule{

        int generalBenefit = 0 ;

        public GeneralRule(byte indexType) {
            super(indexType);
        }
    }


    public class SpecificRule extends Rule{

        public final int first;
        public final int second;
        public int cost;

        public SpecificRule(byte indexType, int first , int second) {
            super(indexType);
            this.first = first;
            this.second = second;
        }
    }
}
