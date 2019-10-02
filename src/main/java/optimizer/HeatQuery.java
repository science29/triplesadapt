package optimizer;

import QueryStuff.Query;
import QueryStuff.VertexGraph;
import org.mapdb.Atomic;
import triple.Triple;
import triple.TriplePattern;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HeatQuery {


    private HashMap<Integer, QueryElement> predicateHeatElmentMap = new HashMap<>();

    public void addQuery(Query query){
        for(int i =0 ; i < query.triplePatterns2.size() ; i++){
            TriplePattern2 pattern = query.triplePatterns2.get(i);
            addQuery(pattern);
        }

    }

    private void addQuery(TriplePattern2 triplePattern) {
        QueryElement queryElement = predicateHeatElmentMap.get(triplePattern.getTriples()[1]);
        if (queryElement == null) {
            queryElement = new QueryElement(triplePattern);
            predicateHeatElmentMap.put(triplePattern.getTriples()[1] , queryElement);
        }
        for (int i = 0; i < triplePattern.getLefts().size(); i++) {
            HeatElement leftHeat = queryElement.left.get(triplePattern.getLefts().get(i).getTriples()[1]);
            if(leftHeat == null)
                leftHeat = queryElement.newHeat(triplePattern.getLefts().get(i).getTriples()[1],true);
            leftHeat.totalFreq++;
            Integer freqForThatConst = leftHeat.constantsFreqMap.get(triplePattern.getLefts().get(i).getTriples()[0]);
            if (freqForThatConst == null) {
                leftHeat.constantsFreqMap.put(triplePattern.getLefts().get(i).getTriples()[0], 1);
            } else {
                freqForThatConst++;
                leftHeat.constantsFreqMap.put(triplePattern.getLefts().get(i).getTriples()[0], freqForThatConst);
            }
        }



        for (int i = 0; i < triplePattern.getRights().size(); i++) {
            HeatElement rightHeat = queryElement.right.get(triplePattern.getRights().get(i).getTriples()[1]);
            if(rightHeat == null)
                rightHeat = queryElement.newHeat(triplePattern.getRights().get(i).getTriples()[1] , false);
            rightHeat.totalFreq++;
            Integer freqForThatConst = rightHeat.constantsFreqMap.get(triplePattern.getRights().get(i).getTriples()[2]);
            if (freqForThatConst == null) {
                rightHeat.constantsFreqMap.put(triplePattern.getRights().get(i).getTriples()[2], 1);
            } else {
                freqForThatConst++;
                rightHeat.constantsFreqMap.put(triplePattern.getRights().get(i).getTriples()[2], freqForThatConst);
            }
        }
    }

    private class QueryElement {

        final TriplePattern2 triplePattern;
        HashMap<Integer, HeatElement> right;
        HashMap<Integer, HeatElement> left;

        public QueryElement(TriplePattern2 triplePattern2){
            this.triplePattern = triplePattern2;
            right = new HashMap<>();
            left = new HashMap<>();
        }

        public HeatElement newHeat(int predicate , boolean addLeft) {
            HeatElement heatElement = new HeatElement();
            if(addLeft)
                left.put(predicate , heatElement);
            else
                right.put(predicate , heatElement);
            return heatElement;
        }
    }

    private class HeatElement {
        int totalFreq;
        HashMap<Integer, Integer> constantsFreqMap;
        public HeatElement(){
            totalFreq = 0;
            constantsFreqMap = new HashMap<>();
        }
    }



}
