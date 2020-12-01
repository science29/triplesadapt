package optimizer;

import index.IndexesPool;
import optimizer.Rules.GeneralRule;

import java.util.*;

public class Simu {


    private double relpcationCostSave;
    private int totalCapacity;

    public Simu(double relpcationCostSave , int totalCapacity) {
        ArrayList<GeneralRule> generalRules;
        ArrayList<Optimiser.SpecificRule> genralizedRules;
        ArrayList<Optimiser.SpecificRule> specificRules;
        this.relpcationCostSave = relpcationCostSave;
        this.totalCapacity = totalCapacity*1000*1000;
    }


    public void buildGeneralRules(int totalTriples, double qualityRatio, double localityRatio, double length,
                                  double s_boundedRatio, double boundedRatio, double replicationUsage, double borderSizeRatio,
                                  double pSelectivtyLog) {
        ArrayList<GeneralRule> generalRules = new ArrayList<>();
       // HashMap<GeneralRule , Double> ruleLeftSize = new HashMap<>();
        GeneralRule baseRuleBounded;
        GeneralRule basePredicateRule;
        GeneralRule secondRuleBounded;
        GeneralRule ppCahceRule = new GeneralRule(IndexesPool.PPx , null);
        //double pSelectivtyLog = 20;
        if (s_boundedRatio >= 0.5) {
            baseRuleBounded = new GeneralRule(IndexesPool.SP_o, null);
            basePredicateRule = new GeneralRule(IndexesPool.POs, null);
            secondRuleBounded = new GeneralRule(IndexesPool.OPs , null);
            secondRuleBounded.setRelativeUsage(((1-s_boundedRatio)*boundedRatio + (1-boundedRatio)/2));
            secondRuleBounded.setGeneralAbsoulteBenefit(pSelectivtyLog * length );
        } else {
            baseRuleBounded = new GeneralRule(IndexesPool.OP_s, null);
            basePredicateRule = new GeneralRule(IndexesPool.PSo, null);
            secondRuleBounded = new GeneralRule(IndexesPool.SPo , null);
            secondRuleBounded.setRelativeUsage((s_boundedRatio)*boundedRatio + (1-boundedRatio)/2);
            secondRuleBounded.setGeneralAbsoulteBenefit(pSelectivtyLog * length );
        }

        if(totalCapacity/totalTriples > 3){
            ppCahceRule.setGeneralAbsoulteBenefit(pSelectivtyLog*3);//not sure
            ppCahceRule.setRelativeUsage(1-boundedRatio);
            ppCahceRule.expectedSize = (int)(totalTriples*pSelectivtyLog);
            generalRules.add(ppCahceRule);
        }

        basePredicateRule.setBaseLineIndex(true);
        baseRuleBounded.setBaseLineIndex(true);


        generalRules.add(baseRuleBounded);
        generalRules.add(basePredicateRule);
        generalRules.add(secondRuleBounded);

        secondRuleBounded.calculateBenefit();




       // ruleLeftSize.put(secondRuleBounded , secondRuleBounded.getGeneralBenefit());
        ArrayList<EvictItem> indexEvictItems = generateEvictItems(generalRules, totalTriples, localityRatio, -1);


        GeneralRule replicationGeneralRule = new GeneralRule((byte) 0 , null) ;
        replicationGeneralRule.setRelativeUsage(replicationUsage/1.0);
        replicationGeneralRule.setGeneralAbsoulteBenefit(relpcationCostSave);

        GeneralRule replicationGeneralRule2 = new GeneralRule((byte) 0 , null) ;
        replicationGeneralRule2.setRelativeUsage(replicationUsage/2.0);
        replicationGeneralRule2.setGeneralAbsoulteBenefit(relpcationCostSave);

        GeneralRule replicationGeneralRule3 = new GeneralRule((byte) 0 , null) ;
        replicationGeneralRule3.setRelativeUsage(replicationUsage/3.0);
        replicationGeneralRule3.setGeneralAbsoulteBenefit(relpcationCostSave);

        ArrayList<GeneralRule> replicationRules = new ArrayList<>();
        replicationRules.add(replicationGeneralRule);
        if(length > 2)
            replicationRules.add(replicationGeneralRule2);
        if(length > 3)
            replicationRules.add(replicationGeneralRule3);

        ArrayList<EvictItem> replicationEvictItems =  generateEvictItems(replicationRules,(int)(totalTriples * borderSizeRatio),localityRatio, 1.5);

        finalizeSize( indexEvictItems , replicationEvictItems , totalCapacity);
    }

    private void finalizeSize(ArrayList<EvictItem> indexEvictItems, ArrayList<EvictItem> replicationEvictItems , int totalCapacity ){
        int usedCapacity = 0;
       // int ii = 0 , ri =0;
        //first do the basic indexes
        for(int i = 0 ; i < indexEvictItems.size() && usedCapacity < totalCapacity ; i++){
            if(indexEvictItems.get(i).rule.isBaseLineIndex())
                usedCapacity += indexEvictItems.get(i).assignFull() *indexEvictItems.get(i).unitSize ;
        }

        while (usedCapacity < totalCapacity){
            EvictItem evictItemNextIndex = getNext(indexEvictItems);
            EvictItem evictItemNextReplication =  getNext(replicationEvictItems);

            double benefitIndex = 0  , replicaBenefit = 0;
            if(evictItemNextIndex != null)
                benefitIndex = evictItemNextIndex.getNext();
            if(evictItemNextReplication != null)
                replicaBenefit = evictItemNextReplication.getNext();

            if(replicaBenefit == 0 && benefitIndex == 0){
                break;
            }
            if(benefitIndex > replicaBenefit) {
                evictItemNextIndex.assignedUnit();
                usedCapacity += evictItemNextIndex.unitSize;
            }else{
                evictItemNextReplication.assignedUnit();
                usedCapacity += evictItemNextReplication.unitSize;
            }
        }

        if(usedCapacity >= totalCapacity){
            System.out.println("no more capacity left..");
        }else
            System.out.println("left:"+(totalCapacity - usedCapacity));

        print(indexEvictItems , replicationEvictItems);
    }



    private EvictItem getNext(ArrayList<EvictItem> evictItems){
        EvictItem maxItem = null;
        for(int i = 0 ; i < evictItems.size() ; i++){
            if(evictItems.get(i).rule.isBaseLineIndex())
                continue;
            if(evictItems.get(i).hasNext()){
                if(maxItem == null)
                    maxItem = evictItems.get(i);
                else
                    if(evictItems.get(i).getNext() > maxItem.getNext()){
                    maxItem = evictItems.get(i);
                }
            }

        }
        return maxItem;
    }



    private void print(ArrayList<EvictItem> indexEvictItems, ArrayList<EvictItem> replicationEvictItems) {
        for(int i =0 ; i < indexEvictItems.size() ; i++){
           String indexName = IndexesPool.getIndexName(indexEvictItems.get(i).rule.indexType);
           System.out.println(indexName+":"+(indexEvictItems.get(i).assignedUnitCount*indexEvictItems.get(i).unitSize)/1000000.0 +",unitSize"+indexEvictItems.get(i).unitSize +
                   ",units"+indexEvictItems.get(i).unitCount+", assigend units:"+indexEvictItems.get(i).assignedUnitCount);
        }
        double repC = 0;
        for(int i =0 ; i < replicationEvictItems.size() ; i++){
          //  String indexName = IndexesPool.getIndexName(replicationEvictItems.get(i).rule.indexType);
           // System.out.println("replication:"+replicationEvictItems.get(i).assignedUnitCount*replicationEvictItems.get(i).unitSize);
            double repT = replicationEvictItems.get(i).assignedUnitCount * replicationEvictItems.get(i).unitSize;
            System.out.println("replication"+": "+(repT)/1000000.0 +",unitSize "+replicationEvictItems.get(i).unitSize +
                    ",units"+replicationEvictItems.get(i).unitCount+", assigend units: "+replicationEvictItems.get(i).assignedUnitCount);
            repC += repT;
        }
        System.out.println("total replication: "+repC/1000000.0);
        System.exit(0);
    }


    private ArrayList<EvictItem> generateEvictItems(ArrayList<GeneralRule> generalRules, int totalTriples , double ratio , double replicationIncreaseRatio){
        ArrayList<EvictItem> evictItems = new ArrayList<>();

        for(int i =0 ; i < generalRules.size() ; i++){
            if(replicationIncreaseRatio > 0)
                totalTriples = (int)(totalTriples * replicationIncreaseRatio);
            int usedTotal = totalTriples;
            double usedRatio = ratio;
            if(generalRules.get(i).expectedSize != 0) {
                usedTotal = generalRules.get(i).expectedSize;
                usedRatio = ratio * 0.05;
            }
            evictItems.add(new EvictItem(generalRules.get(i) , usedRatio , usedTotal));
        }

        return evictItems;

    }


    private class EvictItem{
        int size;
        double benefit;
        GeneralRule rule;
        int unitCount;
        double benefitStep;
        double normalDeviaion;
        HashMap<Integer , Double> unitIDBenefitMap = new HashMap<>();
        ArrayList<Integer> sortedUnits = new ArrayList<>();

        private int unitSize = 10000;
        private int doneSize = 0;
        private int assignedUnitCount = 0;
        private int currenSpecificIndex =0;

        public EvictItem(GeneralRule rule, double ratio , int totalTriples) {
            this.rule = rule;
            this.size = totalTriples;
            this.unitCount = size/unitSize;
            this.normalDeviaion = ratio * unitCount/2.0;
            if(rule != null) {
                rule.calculateBenefit();
                this.benefit = rule.getGeneralBenefit();
            }
            this.benefitStep = benefit/unitCount;
            work();
        }

        public double getNext(){
            if(currenSpecificIndex < sortedUnits.size() && unitIDBenefitMap.containsKey(sortedUnits.get(currenSpecificIndex))
                    && unitIDBenefitMap.get(sortedUnits.get(currenSpecificIndex)) > benefitStep) {
                Double benefit = unitIDBenefitMap.get(sortedUnits.get(currenSpecificIndex));
                return benefit;
            }
            return benefitStep;
        }

        public void assignedUnit(){
            currenSpecificIndex++;
            assignedUnitCount++;
        }

        public boolean hasNext(){
            return assignedUnitCount < unitCount;
        }


        public int assignFull() {
            int diff = unitCount-assignedUnitCount;
            assignedUnitCount = unitCount;
            currenSpecificIndex = assignedUnitCount;
            return diff;
        }


        private void work(){
            double totalAssignedBenefit = 0;
            Random random = new Random();
            while (totalAssignedBenefit < benefit) {
                double p = Math.abs(random.nextGaussian()* normalDeviaion);
                if(p > unitCount)
                    continue;
                Double idB = unitIDBenefitMap.get((int)p);
                if(idB == null)
                    idB = new Double(0);
                unitIDBenefitMap.put((int)p , idB+benefitStep);
                totalAssignedBenefit += benefitStep;

            }

            Iterator<Map.Entry<Integer, Double>> iterator = unitIDBenefitMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<Integer, Double> entry = iterator.next();
                sortedUnits.add(entry.getKey());
            }

            Collections.sort(sortedUnits, new Comparator<Integer>() {
                // @Override
                public int compare(Integer lhs, Integer rhs) {
                    // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                    if (unitIDBenefitMap.get(lhs) < unitIDBenefitMap.get(rhs))
                        return 1;
                    if (unitIDBenefitMap.get(lhs) > unitIDBenefitMap.get(rhs))
                        return -1;
                    return 0;
                    // return lhs.customInt > rhs.customInt ? -1 : (lhs.customInt < rhs.customInt) ? 1 : 0;
                }
            });

        }


    }





}
