package optimizer;

import QueryStuff.QueryStreamGenerator;
import QueryStuff.QueryWorkersPool;
import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import index.MyHashMap;
import optimizer.GUI.OptimizerGUI;
import optimizer.Replication.BorderReplicationSource;
import optimizer.Replication.Replication;
import optimizer.Rules.GeneralReplicationInfo;
import optimizer.Rules.GeneralReplicationRule;
import optimizer.Rules.GeneralRule;
import triple.Triple;

import java.util.*;

public class Optimizer2 extends  Optimiser{

    //private final IndexesPool indexesPool;
    public final GeneralReplicationInfo genralReplicationInfo;
    private final HashMap<Integer, Boolean> borderTripleMap;
    private final QueryStreamGenerator queryGenerator;

    public ArrayList<GeneralRule> generalRule;
    public ArrayList<GeneralReplicationRule> generalReplicationRules;
    public long totalIndexUsage = 0;
    public Replication replication;

    Evictor2 evictor ;
    private long totalIndexSize;
    private  final Transporter transporter;

    private OptimizerGUI GUI;

    public Optimizer2(QueryWorkersPool queryWorkersPool, IndexesPool indexesPool, Dictionary dictionary, Transporter transporter, HashMap<Integer, Boolean> borderTripleMap){
        super(dictionary,indexesPool);
        this.transporter = transporter;
        generalRule = new ArrayList<>();
        generalRule.add(new GeneralRule(IndexesPool.OPs ,SourceSelection.getLocalIndexInstance(null)));
        generalRule.add(new GeneralRule(IndexesPool.SPo,SourceSelection.getLocalIndexInstance(null)));
        generalRule.add(new GeneralRule(IndexesPool.OSp,SourceSelection.getLocalIndexInstance(null)));
        generalRule.add(new GeneralRule(IndexesPool.SOp,SourceSelection.getLocalIndexInstance(null)));
        generalRule.add(new GeneralRule(IndexesPool.POs,SourceSelection.getLocalIndexInstance(null)));
        generalRule.add(new GeneralRule(IndexesPool.PSo,SourceSelection.getLocalIndexInstance(null)));

        generalReplicationRules = new ArrayList<>();
        genralReplicationInfo = new GeneralReplicationInfo(transporter.getSendingItemCostMB());
        SourceSelection source = SourceSelection.getBorderReplicationInstance(new BorderReplicationSource());
        generalReplicationRules.add(new GeneralReplicationRule(IndexesPool.SPo, genralReplicationInfo ,source ));


        this.borderTripleMap = borderTripleMap;

        evictor = new Evictor2(queryWorkersPool , indexesPool, dictionary , this);
        replication = new Replication(this.transporter, borderTripleMap, indexesPool , this);
        this.queryGenerator = QueryStreamGenerator.getDefault(transporter,queryWorkersPool,dictionary,indexesPool, heatQuery,this);
        GUI = OptimizerGUI.createForm(queryGenerator , this);
    }


    public void addStartTripleToIndex(Triple tripleObj , int cnt) {
        //Applying zero protocol
        //initially add to SPo and POs then do latter do other
        indexesPool.addToIndex(IndexesPool.SPo , tripleObj);
        indexesPool.addToIndex(IndexesPool.OPs , tripleObj);
        indexesPool.addToIndex(IndexesPool.PSo , tripleObj);
        if(cnt % 1000 == 0 || cnt == -1)
            GUI.setIndexes(indexesPool);
    }

    public void work(){
        evictor.evict();
        workGeneral();
    }



    private void workGeneral() {

        //set the benefit
        int totalIndexSizeT = 0;
        ArrayList<GeneralRule> unifiedList = new ArrayList<>();

        for(int i = 0; i < generalRule.size() ; i++){
            GeneralRule generalRule = this.generalRule.get(i);
            unifiedList.add(generalRule);
            //byte nearestIndex = indexesPool.getNearestIndex(generalRuleOld.indexType, true);
            double cost = getGenralConversionCost(generalRule.indexType , false); //TODO consider using the cache
            generalRule.calculateGeneralBenefit(cost);
            generalRule.occupation = indexesPool.getIndex(generalRule.indexType).size();
            totalIndexSizeT += generalRule.occupation;
        }
        totalIndexSize = totalIndexSizeT;

        //now do the replication
        for(int i = 0 ; i < generalReplicationRules.size() ; i++){
            generalReplicationRules.get(i).calculateBenefit();
            unifiedList.add(generalReplicationRules.get(i));
        }


        //then sort
        Collections.sort(unifiedList, new Comparator<GeneralRule>() {
            // @Override
            public int compare(GeneralRule lhs, GeneralRule rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                if(lhs.getGeneralBenefit() < rhs.getGeneralBenefit())
                    return 1;
                if(lhs.getGeneralBenefit() > rhs.getGeneralBenefit())
                    return -1;
                return 0;
                // return lhs.customInt > rhs.customInt ? -1 : (lhs.customInt < rhs.customInt) ? 1 : 0;
            }
        });



        //then generate the eviction elements
        evictor.clear();
        for(int i = 0; i < unifiedList.size() ; i++){
            GeneralRule generalRule = unifiedList.get(i);
            double evictBenefit = (generalRule.usage/totalIndexUsage) - generalRule.occupation/totalIndexSize ;
            EvictQueueElement evictQueueElement = new EvictQueueElement(generalRule.scope , evictBenefit , generalRule.indexType , transporter);
            if((generalRule.occupation / totalIndexSize) < (generalRule.usage/totalIndexUsage)){
                evictor.addToHighQueue(evictQueueElement);
            }else if((generalRule.occupation / totalIndexSize) > (generalRule.usage/totalIndexUsage)){
                evictor.addToLowQueue(evictQueueElement);
            }
        }

        //finally inform the evictor

        evictor.evict();

    }



    private HashMap<Byte , Long> cachedCost = new HashMap<>();

    private double getGenralConversionCost(byte optimalIndex , boolean useCache) {

        if(useCache && cachedCost.containsKey(optimalIndex)) {
           //check if nothing has changed
            return cachedCost.get(optimalIndex);
        }

        //get random triples then measure the cost of retrieveing them
        MyHashMap<Integer,ArrayList<Triple>> indexx = indexesPool.getRanIndex();
        Iterator<Map.Entry<Integer, ArrayList<Triple>>> iterator = indexx.entrySet().iterator();

        OperationCost operationCost = new OperationCost();
        int cnt = 0;
        int requiredNumberOfChecks = 100;
        while (iterator.hasNext() && cnt < requiredNumberOfChecks){
            ArrayList<Triple> list = iterator.next().getValue();
            Triple triple = list.get(0);
            indexesPool.get(triple.triples[indexesPool.getFirstIndex(optimalIndex)] ,
                    triple.triples[indexesPool.getSecondIndex(optimalIndex)] , indexesPool.getFirstIndex(optimalIndex) ,
                    indexesPool.getSecondIndex(optimalIndex), operationCost );
            cnt++;
        }
        operationCost.cost = operationCost.cost/cnt;
        cachedCost.put(optimalIndex , operationCost.cost) ;
        return operationCost.cost;
    }


    public void reportIndexUsage(byte indexType , int usage){
        for(int i = 0; i < generalRule.size() ; i++){
           if(generalRule.get(i).indexType  == indexType){
               generalRule.get(i).usage += usage;
               totalIndexUsage++;
           }
        }
    }

    public void reportReplicationUsage(int usedLength , int queryLenght){
        genralReplicationInfo.inform(usedLength , queryLenght);
    }

    public void sendRequiredReplications(int toId, int dist, int from, int to) {
        replication.sendRequiredReplications(toId , dist , from , to);
    }

    public byte getReplicationDestiIndexType() {
        //TODO should be grantied to be sorted
        return generalRule.get(0).indexType;
    }


    //during the partitioing , i'm been assigned my share
    public void recievedIndexBatch(int[] arr, byte indexType) {
        if(arr == null)//TODO this means its done
            return;
        MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(indexType);
        ArrayList<Triple> list = new ArrayList<Triple>();
        int prev = Math.abs(arr[0]);
        for(int i = 0 ; i < arr.length ; i++){
            if(arr[i] < 0) {
                arr[i] = -1 * arr[i];
                addToBorderMap(arr[i]);
            }
            if(arr[i] != prev){
                addShareToIndex(index, list, prev);
                list = new ArrayList<Triple>();
                prev = arr[i];
            }
            list.add(new Triple(arr[i],arr[i+1],arr[i+2]));
            i+=2;
        }
        addShareToIndex(index, list, prev);
    }

    private void addToBorderMap(int v) {
        borderTripleMap.put(v,true);
    }


    private void addShareToIndex(MyHashMap<Integer, ArrayList<Triple>> index, ArrayList<Triple> list, int key) {
        if(!index.containsKey(key))
            index.put(key , list);
        else{
            ArrayList<Triple> alreadyList = index.get(key);
            for(int j = 0 ; j < list.size() ; j++){
                alreadyList.add(list.get(j));
            }
        }
    }


    public class OperationCost{
        public long cost;
        public final long DATA_SCAN = 1000000000; //TODO: to be changed!!
        public OperationCost(){
            cost = -1;
        }

        public void addNoSuitableIndexCost() {
            cost += DATA_SCAN;
        }
    }



}
