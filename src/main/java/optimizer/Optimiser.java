package optimizer;

import QueryStuff.Query;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;

public class Optimiser {


    private final MemoryMap memoryMap;
    private final HeatQuery heatQuery;


    public Optimiser() {
        memoryMap = new MemoryMap();
        heatQuery = new HeatQuery();
    }


    public void informGenralIndexUsage(byte index , int count){
        memoryMap.informIndexUsage(index , count);
    }

    public void informIndexUsage(byte index , int count , TriplePattern2 pattern){
        memoryMap.informSpecificIndexUsage(index , count , pattern);
    }

    public void addQuery(Query query) {
        heatQuery.addQuery(query);

    }


    public class MemoryMap{
        private final ArrayList<Rule> rulesList;
        private final HashMap<Byte , Rule> rulesMap;

        public MemoryMap() {
            this.rulesList = new ArrayList<>();
            this.rulesMap = new HashMap<>();
        }


        public void informIndexUsage(byte index , int count) {
            Rule rule = rulesMap.get(index);
            if(rule == null) {
                rule = new GeneralRule(index);
                rulesMap.put(index , rule);
            }
            rule.usage += count;
        }

        public void informSpecificIndexUsage(byte index, int count, TriplePattern2 pattern) {
            Rule rule = rulesMap.get(index);
            if(rule == null) {
                rule = new SpecificRule(index, pattern);
                rulesMap.put(index , rule);
            }
            rule.usage += count;
        }
    }

    public abstract class Rule{
        public final byte indexType;
        public int occupation;
        public int usage;
        public Rule(byte indexType) {
            this.indexType = indexType;
        }
    }

    public class GeneralRule extends Rule{

        int generalBenefit;

        public GeneralRule(byte indexType) {
            super(indexType);
        }
    }


    public class SpecificRule extends GeneralRule{

        public final TriplePattern2 pattern;
        public int cost;

        public SpecificRule(byte indexType, TriplePattern2 pattern) {
            super(indexType);
            this.pattern = pattern;
        }
    }

}
