package optimizer;

import QueryStuff.Query;
import index.MyHashMap;
import triple.Triple;
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

    public void informIndexUsage(byte index , int count , int first , int second){
        memoryMap.informSpecificIndexUsage(index , count , first ,second);
    }

    public void addQuery(Query query) {
        heatQuery.addQuery(query);

    }

    public void informOptimalIndexUsage(MyHashMap<Integer, ArrayList<Triple>> optimal, int first, int second) {
        informGenralIndexUsage(optimal.poolRefType ,1);
        informIndexUsage(optimal.poolRefType ,1,first , second);
    }

    public void informSubOptIndexUsage(MyHashMap<Integer, ArrayList<Triple>> index, MyHashMap<Integer, ArrayList<Triple>> optimal, int first, int second , int benefit) {
        memoryMap.informGenBenefitOfOptimal(optimal.poolRefType , benefit);
    }


    public class MemoryMap{
        private final ArrayList<Rule> rulesList;
        private final HashMap<Byte , SpecificRule> specificRulesMap;
        private final HashMap<Byte , GeneralRule> generalRulesMap;

        public MemoryMap() {
            this.rulesList = new ArrayList<>();
            this.generalRulesMap = new HashMap<>();
            this.specificRulesMap = new HashMap<>();
        }


        public void informGenBenefitOfOptimal(byte index , int benefit ){
            GeneralRule rule = generalRulesMap.get(index);
            if(rule == null) {
                rule = new GeneralRule(index);
                generalRulesMap.put(index , rule);
            }
            rule.generalBenefit += benefit;

        }

        public void informIndexUsage(byte index , int count) {
            GeneralRule rule = generalRulesMap.get(index);
            if(rule == null) {
                rule = new GeneralRule(index);
                generalRulesMap.put(index , rule);
            }
            rule.usage += count;
        }

        public void informSpecificIndexUsage(byte index, int count, int first , int second) {
            SpecificRule rule = specificRulesMap.get(index);
            if(rule == null) {
                rule = new SpecificRule(index, first , second);
                specificRulesMap.put(index , rule);
            }
            rule.usage += count;
        }
    }

    public abstract class Rule{
        public final byte indexType;
        public int occupation = 0;
        public int usage  = 0 ;
        public Rule(byte indexType) {
            this.indexType = indexType;
        }
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
