package optimizer;

import QueryStuff.Query;
import index.Dictionary;
import index.IndexesPool;
import index.MyHashMap;
import triple.Triple;
import triple.TriplePattern2;

import java.util.*;

public class Optimiser {


    public static final int FULL_DATA_COST = 1000000000; //TODO consider adjusting it


    private final MemoryMap memoryMap;
    private final HeatQuery heatQuery;
    private final IndexesPool indexesPool;

    public Optimiser(Dictionary dictionary) {
        memoryMap = new MemoryMap();
        heatQuery = new HeatQuery();
        indexesPool = new IndexesPool(null , dictionary );
        intialZeroProtocol();
    }


    public void informGenralIndexUsage(byte index , int count){
        memoryMap.informIndexUsage(index , count);
    }

    public void informIndexUsage(byte index , int count , Integer first , Integer second){
        memoryMap.informSpecificIndexUsage(index , count , first ,second);
    }

    public void addQuery(Query query) {
        heatQuery.addQuery(query);

    }

    public void informOptimalIndexUsage(MyHashMap<Integer, ArrayList<Triple>> optimal, Integer first, Integer second) {
        informGenralIndexUsage(optimal.poolRefType ,1);
        informIndexUsage(optimal.poolRefType ,1,first , second);
    }

    public void informSubOptIndexUsage(MyHashMap<Integer, ArrayList<Triple>> index, MyHashMap<Integer, ArrayList<Triple>> optimal, int first, int second , int benefit) {
        memoryMap.informGenBenefitOfOptimal(optimal.poolRefType , benefit);
    }



    private Set<Byte> indexSet = new LinkedHashSet<>();

    private void intialZeroProtocol(){
        indexSet.add(IndexesPool.SPo);
        indexSet.add(IndexesPool.POs);
        indexSet.add(IndexesPool.OPs);
        indexSet.add(IndexesPool.PSo);
        indexSet.add(IndexesPool.SOp);
        indexSet.add(IndexesPool.OSp);

        //TODO the replication stuff ..

    }

    public void addStartTripleToIndex(Triple tripleObj) {
        //Applying zero protocol
        //initially add to SPo and POs then do latter do other
        indexesPool.addToIndex(IndexesPool.SPo , tripleObj);
    }



    public void dataReadDone(){
        Byte startType = null;
        Iterator iterator = indexSet.iterator();
        while (iterator.hasNext()){
            if(startType == null){
                startType = (Byte) iterator.next();
                continue;
            }
            Byte indexType = (Byte) iterator.next();
            boolean more = indexesPool.buildIndex(startType , indexType);
            if(!more)
                break;
        }
        indexesPool.sortAllSortedIndexes();
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
                rulesList.add(rule);
            }
            rule.usage += count;
        }

        public void informSpecificIndexUsage(byte index, int count, Integer first , Integer second) {
            SpecificRule rule = specificRulesMap.get(index);
            if(rule == null) {
                rule = new SpecificRule(index, first , second);
                specificRulesMap.put(index , rule);
                rulesList.add(rule);
            }
            rule.usage += count;
        }

        public int getHighBenefit(int quantity){
            sortRuleList();
            int toMoveCount = 0 ;
            for(int i = 0 ; i < rulesList.size() ; i++){
                 //if(rulesList.get(i) instanceof GeneralRule)
                rulesList.get(i).evictUp(quantity);


            }

        }

    }

    private byte getStartIndexType(byte indexType) {

    }

    public abstract class Rule{
        public final byte indexType;
        public int occupation = 0;
        public boolean moreToBuild = true;
        public int usage  = 0 ;
        public int effectiveness = 0 ;
        public Rule(byte indexType) {
            this.indexType = indexType;
        }

        public void countAddedToMemory(int count){
            occupation += count;
        }

        public void setMoreToBuild(boolean more) {
            moreToBuild = more;
        }

        public abstract void evictUp(int quantity);



    }

    public class GeneralRule extends Rule{

        int generalBenefit = 0 ;
        Iterator addIterator;
        Iterator removeIterator;
        int totalEvictedOut = 0;

        public GeneralRule(byte indexType) {
            super(indexType);
        }

        public void evictUp(int quantity){
            byte startIndexType = getStartIndexType(indexType);
            if(totalEvictedOut > 0 && quantity < totalEvictedOut) {//TODO test this
                int currentOcuupation = occupation;
                indexesPool.buildIndex(startIndexType, indexType, null, quantity, this);
                totalEvictedOut -= (occupation - currentOcuupation);
                quantity -= (occupation - currentOcuupation);
            }
            addIterator = indexesPool.buildIndex(startIndexType , indexType , addIterator , quantity, this);
        }

        public void evictOut(int quantity){
            MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(indexType);
            removeIterator  = index.entrySet().iterator();
            int count = 0;
            ArrayList<Integer> listToRemove = new ArrayList<>();
            while (removeIterator.hasNext()){
                Map.Entry pair = (Map.Entry)removeIterator.next();
                Integer key = (Integer)pair.getKey();
                if(isSpecific(index , key))
                    continue;
                listToRemove.add(key);
                count += index.get(key).size();
                if(count > quantity)
                    break;
            }

            for(int i = 0 ; i < listToRemove.size() ; i++){
                index.remove(listToRemove.get(i));
            }
            if(count > 0) {
                moreToBuild = true;
                occupation -= count;
                totalEvictedOut += count;
            }


        }


    }



    public class SpecificRule extends Rule{

        public final Integer first;
        public final Integer second;
        public int cost;

        public SpecificRule(byte indexType, Integer first , Integer second) {
            super(indexType);
            this.first = first;
            this.second = second;
        }

        @Override
        public void evictUp(int quantity) {
            indexesPool.get(indexType , first ,second,withiIndex, Optimiser.this);
        }
    }


    private static Triple specificTriple = new Triple(-77 , -66,-55);

    public static  boolean isSpecific(MyHashMap<Integer , ArrayList<Triple>> index , Integer key){
        ArrayList<Triple> list = index.get(key);
        if(list == null || list.size() == 0)
            return false;

        return list.get(0).triples[0] == specificTriple.triples[0];

    }
}
