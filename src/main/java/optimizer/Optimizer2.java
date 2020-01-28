package optimizer;

import QueryStuff.QueryWorkersPool;
import index.Dictionary;
import index.IndexesPool;
import optimizer.Rules.GeneralRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Optimizer2 {

    private final IndexesPool indexPool;
    public ArrayList<GeneralRule> generalRules;
    public long totalIndexUsage = 0;

    Evictor2 evictor ;
    private long totalIndexSize;

    public Optimizer2(QueryWorkersPool queryWorkersPool , IndexesPool indexesPool , Dictionary dictionary){
        generalRules = new ArrayList<>();
        generalRules.add(new GeneralRule(IndexesPool.OPs));
        generalRules.add(new GeneralRule(IndexesPool.SPo));
        generalRules.add(new GeneralRule(IndexesPool.OSp));
        generalRules.add(new GeneralRule(IndexesPool.SOp));
        generalRules.add(new GeneralRule(IndexesPool.POs));
        generalRules.add(new GeneralRule(IndexesPool.PSo));
        
        this.indexPool = indexesPool;

        evictor = new Evictor2(queryWorkersPool , indexesPool, dictionary);
    }


    public void work(){
        evictor.evict();
        workGeneral();
    }

    private void workGeneral() {

        //set the benefit
        int totalIndexSizeT = 0;
        for(int i = 0; i < generalRules.size() ; i++){
            GeneralRule generalRule = generalRules.get(i);
            generalRule.calculateBenefit();
            generalRule.occupation = indexPool.getIndex(generalRule.indexType).size();
            totalIndexSizeT += generalRule.occupation;
        }
        totalIndexSize = totalIndexSizeT;

        //then sort
        Collections.sort(generalRules, new Comparator<GeneralRule>() {
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

        for(int i = 0; i < generalRules.size() ; i++){
            GeneralRule generalRule = generalRules.get(i);
            double evictBenefit = (generalRule.usage/totalIndexUsage) - generalRule.occupation/totalIndexSize ;
            EvictQueueElement evictQueueElement = new EvictQueueElement(null , evictBenefit , generalRule.indexType);
            if((generalRule.occupation / totalIndexSize) < (generalRule.usage/totalIndexUsage)){
                evictor.addToHighQueue(evictQueueElement);
            }else if((generalRule.occupation / totalIndexSize) > (generalRule.usage/totalIndexUsage)){
                evictor.addToLowQueue(evictQueueElement);
            }
        }

        //finally inform the evictor

        evictor.evict();

    }


    public void reportIndexUsage(byte indexType , int usage){
        for(int i = 0 ; i < generalRules.size() ; i++){
           if(generalRules.get(i).indexType  == indexType){
               generalRules.get(i).usage += usage;
               totalIndexUsage++;
           }
        }
    }







}
