package optimizer;

import QueryStuff.Query;
import index.Dictionary;
import index.IndexesPool;
import index.MyHashMap;
import optimizer.Rules.IndexUsage;
import triple.Triple;
import triple.TriplePattern2;

import java.util.*;

public class Optimiser {


    public static final int FULL_DATA_COST = 1000000000; //TODO consider adjusting it


    private final MemoryMap memoryMap;
    protected final HeatQuery heatQuery;
    protected final IndexesPool indexesPool;

    private final Evictor evictor;

    public Optimiser(Dictionary dictionary , IndexesPool indexesPool) {
        memoryMap = new MemoryMap();
        heatQuery = new HeatQuery();
        this.indexesPool = indexesPool;//new IndexesPool(null , dictionary );
        evictor = new Evictor(indexesPool);
        intialZeroProtocol();
    }


    public void informGenralIndexUsage(byte index , int count , TriplePattern2 triplePattern , int potentialFilterCost){
        IndexUsage indexUsage = new IndexUsage(index,count,potentialFilterCost);
        heatQuery.updatePatternUsage(triplePattern,indexUsage , index);
        memoryMap.informIndexUsage(index , count,triplePattern);
    }

    public void informIndexUsage(byte index , int count , Integer first , Integer second, TriplePattern2 triplePattern , int potentialFilterCost){
        memoryMap.informSpecificIndexUsage(index , count , first ,second);
    }

    public void addQuery(Query query) {
        heatQuery.addQuery(query);

    }

    public void informOptimalIndexUsage(MyHashMap<Integer, ArrayList<Triple>> optimal, Integer first, Integer second , TriplePattern2 triplePattern , int potentialFilterCost) {
        informGenralIndexUsage(optimal.poolRefType ,1 , triplePattern , potentialFilterCost);
        informIndexUsage(optimal.poolRefType ,1,first , second , triplePattern , potentialFilterCost );
    }

    public void informSubOptIndexUsage(MyHashMap<Integer, ArrayList<Triple>> index, MyHashMap<Integer, ArrayList<Triple>> optimal, int first, int second , int benefit) {
        memoryMap.informGenBenefitOfOptimal(optimal.poolRefType , index.poolRefType , benefit);
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
        private final HashMap<Byte , ArrayList<GeneralRule_old>> generalRulesMap;

        public MemoryMap() {
            this.rulesList = new ArrayList<>();
            this.generalRulesMap = new HashMap<>();
            this.specificRulesMap = new HashMap<>();
        }


        public void informGenBenefitOfOptimal(byte requiredIndex , byte sourceIndex , int benefit ){
            ArrayList<GeneralRule_old> rules = generalRulesMap.get(requiredIndex);
            if(rules == null) {
                SourceItem sourceItem = new SourceItem(sourceIndex , Optimiser.LOCALHOST);
                GeneralRule_old rule = new GeneralRule_old(requiredIndex , sourceItem) ;
                rules = new ArrayList<>();
                rules.add(rule);
                generalRulesMap.put(requiredIndex , rules);
                rulesList.add(rule);
            }
            for(int i =  0 ; i < rules.size() ; i++){
                if(rules.get(i).source == null || rules.get(i).source.equals(sourceIndex))
                    rules.get(i).generalBenefit += benefit;
            }
        }

        public void informIndexUsage(byte index , int count , TriplePattern2 triplePattern) {
            ArrayList<GeneralRule_old> rules = generalRulesMap.get(index);
            if(rules == null) {
                GeneralRule_old rule = new GeneralRule_old(index , null) ;
                rules = new ArrayList<>();
                rules.add(rule);
                generalRulesMap.put(index , rules);
                rulesList.add(rule);
            }
            for(int i =  0 ; i < rules.size() ; i++){
                rules.get(i).usage += count;
            }
        }

        public void informSpecificIndexUsage(byte index, int count, Integer first , Integer second) {
            SpecificRule rule = specificRulesMap.get(index);
            if(rule == null) {
                rule = new SpecificRule(index, first , second );
                specificRulesMap.put(index , rule);
                rulesList.add(rule);
            }
            rule.usage += count;
        }

       /* public int getHighBenefit(int quantity){
            sortRuleList();
            int toMoveCount = 0 ;
            for(int i = 0 ; i < rulesList.size() ; i++){
                 //if(rulesList.get(i) instanceof GeneralRule_old)
                rulesList.get(i).evictUp(quantity);


            }

        }*/

    }

    private byte getStartIndexType(byte indexType , SourceItem sourceItem ) {
        //TODO consider if it is not localhost
        return sourceItem.sourceIndex;
    }

    public abstract class Rule{
        public final byte indexType;
        public int occupation = 0;
        public boolean moreToBuild = true;
        public int usage  = 0 ;
        public int effectiveness = 0 ;
        final SourceItem source;
        public Rule(byte indexType , SourceItem source) {
            this.indexType = indexType;
            this.source = source;
        }

        public void countAddedToMemory(int count){
            occupation += count;
        }

        public void setMoreToBuild(boolean more) {
            moreToBuild = more;
        }

        public abstract void evictUp(int quantity);



    }

    @Deprecated
    public class GeneralRule_old extends Rule{

        int generalBenefit = 0 ;
        Iterator addIterator;
        Iterator removeIterator;
        int totalEvictedOut = 0;


        public GeneralRule_old(byte indexType , SourceItem source) {
            super(indexType , source);

        }

        public void evictUp(int quantity){
            byte startIndexType = getStartIndexType(indexType ,source);
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

        public SpecificRule(byte indexType, Integer first , Integer second , byte sourceIndex) {
            super(indexType , new SourceItem(sourceIndex, LOCALHOST));
            this.first = first;
            this.second = second;
        }

        public SpecificRule(byte indexType, Integer first , Integer second ){
            super(indexType , null);
            this.first = first;
            this.second = second;
        }

        @Override
        public void evictUp(int quantity) {
            //TODO it needs to be done all togather becuse it could require scanning the whole data from one index
           ArrayList<Triple> list = indexesPool.get(indexType , first ,-1,null, null , null);
           if(list == null){
               evictor.addToFullScan(this);
           }
           indexesPool.getIndex(indexType).put(first , list);
        }
    }

    public static final int LOCALHOST = -1;

    public class SourceItem{
       public final byte sourceIndex;
       public final  int host;

       public SourceItem(byte sourceIndex , int host){
           this.sourceIndex = sourceIndex;
           this.host = host;
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
