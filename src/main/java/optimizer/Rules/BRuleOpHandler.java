package optimizer.Rules;

import info.uniadapt.api.Assistance;
import optimizer.HeatQuery;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BRuleOpHandler extends  OperationalRule {

    private final int boderDistance;
    private final double genralAccessRate;
    int totalAccess;

    HeatQuery heatQuery;

    public static int delay = 1220;


    public HashMap<Integer, OperationalRule> OperationalRulesMap = new HashMap<>();
    private HashMap<Byte, ArrayList<TriplePattern2>> map;
    private int index = 0;
    private Iterator iterator;

    private byte indexType;


    public BRuleOpHandler(int borderDistance , double gernalAccessRate , byte indexType ){
        this.boderDistance = borderDistance;
        this.genralAccessRate = gernalAccessRate;
        this.indexType = indexType;
    }

    public void serialzie(){
        if(map == null)
            map = new HashMap<>();
        map = heatQuery.serialize(indexType);
    }

    @Override
    public TripleBlock getNextTriplesBlock() {
        ArrayList<Triple> blockList = new ArrayList<>();
        double totalBenefit = 0;
        while (blockList.size() < TripleBlock.block_size) {
            TriplePattern2 pattern = getNextTripleBorderPttern();
            double benefit = heatQuery.getIndexUsage(pattern , indexType).getEffectiveBenefit();
            totalBenefit = totalBenefit + benefit/(genralAccessRate/boderDistance);
            ArrayList<Triple> list = Assistance.evaluate(pattern);
            for (int i = 0; i < list.size(); i++) {
                blockList.add(list.get(i));
            }
        }
        TripleBlock tripleBlock = new TripleBlock(blockList, totalBenefit , indexType);
        return  tripleBlock;

    }


    private TriplePattern2 getNextTripleBorderPttern(){
        if(map == null)
            serialzie();
        if(iterator == null)
            iterator = map.entrySet().iterator();
        double benefitC = 0;
        TriplePattern2 maxPattern = null;
        while (iterator.hasNext()){
            Map.Entry pair = (Map.Entry)iterator.next();
            Byte key = (Byte) pair.getKey();
            ArrayList<TriplePattern2> val = (ArrayList<TriplePattern2>) pair.getValue();
            if(val == null || index < val.size())
                continue;
            for(int i=0; i < val.size() ; i++){
                TriplePattern2 pattern = val.get(0);
                try {
                    if(heatQuery.getIndexUsage(pattern,indexType).borderUsage == 0)
                        val.remove(i);
                }catch (Exception e){
                    continue;
                }
                if(i >= val.size())
                    break;
            }
            TriplePattern2 pattern = val.remove(0);
            if(benefitC < heatQuery.getIndexUsage(pattern,indexType).getEffectiveBenefit() * delay) {
                try {
                    if(heatQuery.getIndexUsage(pattern,indexType).borderUsage ==0)
                        continue;
                    benefitC = heatQuery.getIndexUsage(pattern,indexType).getEffectiveBenefit() * delay;
                }catch (Exception e){
                    continue;
                }
                maxPattern = pattern;
            }
        }
        return maxPattern;

    }




}
