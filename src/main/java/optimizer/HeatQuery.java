package optimizer;

import QueryStuff.Query;
import index.IndexesPool;
import index.MyHashMap;
import org.mapdb.Atomic;
import triple.Triple;
import triple.TriplePattern;
import triple.TriplePattern2;

import java.lang.reflect.Array;
import java.util.*;

public class HeatQuery {


    private HashMap<Integer, ArrayList<Query>> connectedQueriesMap; // key is the a predicate, list of all connected heat queries.
    private ArrayList<Query> connectedQueriesList = new ArrayList<>();
    private HashMap<Integer, QueryElement> predicateHeatElmentMap = new HashMap<>();

    private ArrayList<QueryElement> predicateList = new ArrayList<>();


    private HashMap<Integer, HashMap<Integer, HeatElement2>> heatGraph = new HashMap<>(); // first key is the (from predicate) , the value is map that have (to predicate) as key.


    private void addToPGraph(Integer p1, Integer p2) {
        HashMap<Integer, HeatElement2> opss = heatGraph.get(p1);
        if (opss == null) {
            opss = new HashMap<>();
            opss.put(p2, new HeatElement2());
            heatGraph.put(p1, opss);
            return;
        }
        HeatElement2 in = opss.get(p2);
        if (in != null)
            in.totalFreq++;
        else
            opss.put(p2, new HeatElement2());
    }

    private void addPatternTopGraph(TriplePattern2 tp1, ArrayList<TriplePattern2> triplePatterns) {
        if (triplePatterns == null)
            return;
        for (int j = 0; j < triplePatterns.size(); j++) {
            int p2 = triplePatterns.get(j).getTriples()[1];
            addToPGraph(tp1.getTriples()[1], p2);
        }
    }

    private void addRightLeft(TriplePattern2 callingPattern, TriplePattern2 processPattern) {
        //add left
        addPatternTopGraph(processPattern, processPattern.getLefts());
        for (int i = 0; i < processPattern.getLefts().size(); i++) {
            if (callingPattern != processPattern)
                addRightLeft(processPattern, processPattern.getLefts().get(i));
        }

        //add right
        addPatternTopGraph(processPattern, processPattern.getRights());
        for (int i = 0; i < processPattern.getRights().size(); i++) {
            if (callingPattern != processPattern)
                addRightLeft(processPattern, processPattern.getRights().get(i));
        }
    }


    public void addQuery(Query query) {
        for (int i = 0; i < query.triplePatterns2.size(); i++) {
            TriplePattern2 pattern = query.triplePatterns2.get(i);
            addQuery(pattern);
            addRightLeft(null, pattern);

        }



        /*  Query lastConnectedQuery = null;
        for(int i = 0; i < connectedQueriesList.size() ; i++){
            Integer[] code = new Integer[3];
            code[0] = query.triplePatterns2.get(i).getTriples()[0];
            code[1] = query.triplePatterns2.get(i).getTriples()[1];
            code[2] = query.triplePatterns2.get(i).getTriples()[2];
            if(connectedQueriesList.get(i).addToTriplePatterns(code , null)) {
                if(lastConnectedQuery != null)
                    connectTwoHeatQueries(connectedQueriesList.get(i), lastConnectedQuery);
                lastConnectedQuery = connectedQueriesList.get(i);
            }
        }
        if(lastConnectedQuery == null){
            connectedQueriesList.add(query);
        }
*/
    }

    /*private void connectTwoHeatQueries(Query query1 , Query query2){
        for(int i = 0 ; i < query1.triplePatterns2.size() ; i++){
            Integer[] code = new Integer[3];
            code[0] = query2.triplePatterns2.get(i).getTriples()[0];
            code[1] = query2.triplePatterns2.get(i).getTriples()[1];
            code[2] = query2.triplePatterns2.get(i).getTriples()[2];
            if(query1.addToTriplePatterns(code,null))
                return;
        }
    }*/

    private void addQuery(TriplePattern2 triplePattern) {
        QueryElement queryElement = predicateHeatElmentMap.get(triplePattern.getTriples()[1]);
        if (queryElement == null) {
            queryElement = new QueryElement(triplePattern);
            predicateHeatElmentMap.put(triplePattern.getTriples()[1], queryElement);
            predicateList.add(queryElement);
        }
        for (int i = 0; i < triplePattern.getLefts().size(); i++) {
            HeatElement leftHeat = queryElement.left.get(triplePattern.getLefts().get(i).getTriples()[1]);
            if (leftHeat == null)
                leftHeat = queryElement.newHeat(triplePattern.getLefts().get(i).getTriples()[1], true);
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
            if (rightHeat == null)
                rightHeat = queryElement.newHeat(triplePattern.getRights().get(i).getTriples()[1], false);
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

    Random random = new Random();

    public TriplePattern2 getNextStoredQuery(int length, IndexesPool indexesPool) {
        if(predicateList.size() < 10)
            return null;
        double index = random.nextGaussian() * predicateList.size() / 2;
        index = Math.abs(index);
        if (index >= predicateList.size()) {
            index = predicateList.size() - 1;
        }
        int p = predicateList.get((int) Math.round(index)).triplePattern.getTriples()[1];// all the prediacte that this p pointing to
        ArrayList<Integer> res = new ArrayList<>();
        ArrayList<Triple> tripleRes = new ArrayList<>();
        if (shouldTakeNewVal()) {
            tripleRes = getNextFromIndex(p, indexesPool, length);
        } else {
            getNextPInGraph(p, res, length);
            if (res.size() < length) {
                if(res.size() > 0)
                    p = res.get(0); //TODO the other end ?
                tripleRes = getNextFromIndex(p, indexesPool, length - res.size());
            }

        }
        TriplePattern2 prev = null;
        TriplePattern2 first = null;
        if (res.size() != 0) {
            for (int i = 0; i < res.size() || i < tripleRes.size(); i++) {
                TriplePattern2 triplePattern2;
                if (res.size() == 0) {
                    Integer pr = res.get(i);
                    triplePattern2 = new TriplePattern2(TriplePattern.thisIsVariable(i + 1), pr, TriplePattern.thisIsVariable(i + 2), null, null, null, null);
                } else {
                    Triple pr = tripleRes.get(i);
                    triplePattern2 = new TriplePattern2(TriplePattern.thisIsVariable(i + 1), pr.triples[1], TriplePattern.thisIsVariable(i + 2), null, null, null, null);
                }
                if (i == 0)
                    first = triplePattern2;
                if (prev != null) {
                    triplePattern2.connectTriplePattern(prev, false, true);
                    prev.connectTriplePattern(triplePattern2, true, false);
                }
                prev = triplePattern2;
            }
            return first;
        }
        return null;

    }

    private boolean shouldTakeNewVal() {
        //TODO relate it to the workload quality
        return false;
    }


    Random randomPP = new Random();

    private void getNextPInGraph(Integer p, ArrayList<Integer> res, int length) {
        if (res.size() >= length)
            return;
        HashMap<Integer, HeatElement2> map = heatGraph.get(p);
        if (map == null)
            return;
        //select one of them
        Iterator<Map.Entry<Integer, HeatElement2>> iterator = map.entrySet().iterator();
        int tot = 0;
        while (iterator.hasNext()) {
            HeatElement2 heatElement = iterator.next().getValue();
            tot += heatElement.totalFreq;
        }
        int ch = randomPP.nextInt(tot);
        tot = 0;
        iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            HeatElement2 heatElement = iterator.next().getValue();
            tot += heatElement.totalFreq;
            if (tot >= ch) {
                Integer p2 = iterator.next().getKey();
                res.add(p2);
                getNextPInGraph(p2, res, length);
                return;
            }
        }
    }

    public ArrayList<Triple> getNextFromIndex(Integer p, IndexesPool indexesPool, int requiredLength) {
        MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(IndexesPool.PSo);
        ArrayList<Triple> list = index.get(p);
        ArrayList<Triple> res = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            int o = list.get(i).triples[2];
            goDeeper(o, indexesPool, res, 1, requiredLength, new MaxListInfo());
        }
        return res;
    }

    public boolean goDeeper(Integer s, IndexesPool indexesPool, ArrayList<Triple> res, int currentDeep, int requiredLength, MaxListInfo maxListInfo) {
        if (currentDeep > requiredLength) {
            res.clear();
            return true;
        }
        ArrayList<Triple> listS = indexesPool.getIndex(IndexesPool.SPo).get(s);
        if (listS == null) {
            if (currentDeep > maxListInfo.currentMax) {
                maxListInfo.currentMax = currentDeep;
                maxListInfo.doPut = true;
                res.clear();
            }
            return false;
        }
        boolean addedMax = false;
        for (int j = 0; j < listS.size(); j++) {
            int o = listS.get(j).triples[2];
            if (goDeeper(o, indexesPool, res, currentDeep + 1, requiredLength, maxListInfo)) {
                res.add(0, listS.get(j));
                return true;
            } else if (maxListInfo.doPut) {
                res.add(0, listS.get(j));
                maxListInfo.doPut = false;
                addedMax = true;
            }
        }
        maxListInfo.doPut = addedMax;
        return false;
    }


    private class MaxListInfo {
        int currentMax = -1;
        boolean doPut = false;
    }

    public class QueryElement {

        public final TriplePattern2 triplePattern;
        public HashMap<Integer, HeatElement> right;
        public HashMap<Integer, HeatElement> left;

        public QueryElement(TriplePattern2 triplePattern2) {
            this.triplePattern = triplePattern2;
            right = new HashMap<>();
            left = new HashMap<>();
        }

        public HeatElement newHeat(int predicate, boolean addLeft) {
            HeatElement heatElement = new HeatElement();
            if (addLeft)
                left.put(predicate, heatElement);
            else
                right.put(predicate, heatElement);
            return heatElement;
        }
    }

    public class HeatElement {
        int totalFreq;
        HashMap<Integer, Integer> constantsFreqMap;

        public HeatElement() {
            totalFreq = 0;
            constantsFreqMap = new HashMap<>();
        }
    }

    private class HeatElement2 {
        int totalFreq;
        HashMap<Integer, Integer> constantsFrequencyMap;

        public HeatElement2() {
            constantsFrequencyMap = new HashMap<>();
            totalFreq = 1;
        }
    }


}
