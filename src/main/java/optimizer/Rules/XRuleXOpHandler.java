package optimizer.Rules;

import optimizer.HeatQuery;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static info.uniadapt.api.Assistance.evaluate;

public class XRuleXOpHandler extends  OperationalRule {

    int totalAccess;

    HeatQuery heatQuery;

    public HashMap<Integer, OperationalRule> OperationalRulesMap = new HashMap<>();
    private HashMap<Byte, ArrayList<TriplePattern2>> map;
    private int index = 0;
    private Iterator iterator;

    private  byte indexType;

    public void serialzie(){
        if(map == null)
            map = new HashMap<>();
        map = heatQuery.serialize(indexType);
    }


    public XRuleXOpHandler(Byte indexType){
     this.indexType = indexType;
    }

    @Override
    public TripleBlock getNextTriplesBlock() {
        ArrayList<Triple> blockList = new ArrayList<>();
        double totalBenefit = 0;
        while (blockList.size() < TripleBlock.block_size) {
            TriplePattern2 pattern = getNextTriplePttern();
            double benefit = heatQuery.getIndexUsage(pattern,indexType).getEffectiveBenefit();
            totalBenefit = totalBenefit + benefit;
            ArrayList<Triple> list = evaluate(pattern);
            for (int i = 0; i < list.size(); i++) {
                blockList.add(list.get(i));
            }
        }
        TripleBlock tripleBlock = new TripleBlock(blockList, totalBenefit , indexType);
        return  tripleBlock;

    }


    private TriplePattern2 getNextTriplePttern(){
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
            TriplePattern2 pattern = val.remove(0);
            if(benefitC < heatQuery.getIndexUsage(pattern , indexType).getEffectiveBenefit()) {
                benefitC = heatQuery.getIndexUsage(pattern , indexType).getEffectiveBenefit();
                maxPattern = pattern;
            }
        }
        return maxPattern;

    }




}
