package index;

import optimizer.Optimiser;
import start.MainUinAdapt;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IndexesPool {

    public final static byte Spo = 0;
    public final static byte SPo = 2;
    public final static byte Sop = 3;
    public final static byte SOp = 4;

    public final static byte Pso = 5;
    public final static byte PSo = 6;
    public final static byte Pos = 7;
    public final static byte POs = 8;

    public final static byte Osp = 9;
    public final static byte OSp = 10;
    public final static byte Ops = 11;
    public final static byte OPs = 12;
    private final Dictionary dictionary;


    HashMap<Integer, MyHashMap<Integer,ArrayList<Triple>>> pool;

    final HashMap<Integer , Integer> selectivity;

    final HashMap<Integer , Boolean> isBorder;

    public IndexesPool(HashMap<Integer , Boolean> isBorder , Dictionary dictionary){
        pool = new HashMap<>();
        this.isBorder = isBorder;
        this.dictionary = dictionary;
        selectivity = new HashMap<>();
    }

    public void addIndex(MyHashMap index , int type){
        index.poolRefType = (byte)type;
        pool.put(type , index);
    }


    public MyHashMap<Integer , ArrayList<Triple>> getIndex(int type){
        return pool.get(type);
    }

    public void addIndex(int type, HashMap<Integer, ArrayList<Triple>> map , String name ) {
        MyHashMap<Integer , ArrayList<Triple>> cov = new MyHashMap<Integer, ArrayList<Triple>>(name , map);
        addIndex(cov , type);
    }


    public boolean isBorder(Triple triple , int index){
        return isBorder.containsKey(triple.triples[index]);
    }

    public int getSelectivity(int index){
        return selectivity.get(index);
    }

    public ArrayList<Triple> get(int optimalIndexType , Integer first , Integer second , TriplePattern2.WithinIndex withinIndex , Optimiser optimiser ){
        //first try the optimal
        MyHashMap<Integer,ArrayList<Triple>> optimal = pool.get(optimalIndexType);
        ArrayList<Triple> list;
        int sortedIndex = getSortedIndex(optimalIndexType);
        if(second == -1)
           list =  optimal.get(first);
        else
            list = optimal.get(first , second ,sortedIndex ,withinIndex);
        if(list != null) {
            if(optimiser != null)
                optimiser.informOptimalIndexUsage(optimal , first , second );
            return list;
        }

        //try to find another way
        MyHashMap<Integer, ArrayList<Triple>> index = null;
        if(optimalIndexType == SPo) {
            index = pool.get(PSo);
            sortedIndex = 0;
        }

        if(optimalIndexType == OPs) {
            index = pool.get(POs);
            sortedIndex = 2;
        }
        if(index == null || second < 0) {
            if(optimiser != null)
                optimiser.informSubOptIndexUsage(index,optimal, first , second , Optimiser.FULL_DATA_COST);
            return null;
        }
        list = index.get(second , first ,sortedIndex ,withinIndex);
        int benefit = (int)(withinIndex.cost - Math.log(getSelectivity(first))) ;
        if(optimiser != null)
            optimiser.informSubOptIndexUsage(index,optimal, first , second , benefit);
        if(list != null)
            return list;
        return null;
    }

    private int getSortedIndex(int optimalIndexType) {
        switch (optimalIndexType){
            case OPs: return 1;
            case SPo: return 1;
            case OSp: return 0;
            case SOp: return 2;
            case PSo: return 0;
            case POs: return 2;
        }
        return -1;
    }

    private int getHashedIndex(int optimalIndexType) {
        switch (optimalIndexType){
            case OPs: return 2;
            case SPo: return 0;
            case OSp: return 2;
            case SOp: return 0;
            case PSo: return 1;
            case POs: return 1;
        }
        return -1;
    }



    private int getLastIndex(int indexType) {
        switch (indexType){
            case OPs: return 0;
            case SPo: return 2;
            case OSp: return 1;
            case SOp: return 1;
            case PSo: return 2;
            case POs: return 0;
        }
        return -1;
    }


    public void addToIndex(byte indexType, Triple tripleObj) {
        MyHashMap<Integer, ArrayList<Triple>> index = pool.get(indexType);
        if(index == null){
            index = new MyHashMap<>(indexType+"");
            pool.put(new Integer(indexType),index);
        }

        int key = getHashedIndex(indexType);
       // int sortedKey = getSortedIndex(indexType);

        if (index.containsKey(tripleObj.triples[1])) {
            index.get(tripleObj.triples[1]).add(tripleObj);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(tripleObj);
            Integer codeObj = dictionary.get(dictionary.get(tripleObj.triples[key]));
            index.put(codeObj, list);
        }
    }


    public void sortAllSortedIndexes(){
        Iterator it = pool.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            Integer indexType = (Integer) pair.getKey();
            MyHashMap myHashMap = (MyHashMap) pair.getValue();
            int index1 = getSortedIndex(indexType);
            int index2 = getLastIndex(indexType);
            myHashMap.sort(index1 , index2);
        }
    }



    public boolean buildIndex(byte startIndexType , byte requiredIndexType){
        Iterator it = getIndex(startIndexType).entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            if(count % 1000 == 0){
                if(MainUinAdapt.checkMemory(false) < 1000000)
                    return false;
            }
            count = addCountToIndex(requiredIndexType, it, count);
        }

        return true;
    }

    public Iterator buildIndex(byte startIndexType, byte indexType, Iterator prevoiusIterator, int quantity , Optimiser.Rule rule) {
        Iterator it = prevoiusIterator;
        if(it == null )
            it = getIndex(startIndexType).entrySet().iterator();
        int count = 0;

        while (it.hasNext()) {
            if(count % 1000 == 0){
                if(MainUinAdapt.checkMemory(false) < 1000000)
                    return it;
            }
            if(count >= quantity)
                return it;
            count = addCountToIndex(indexType, it, count);
            if(rule != null)
                rule.countAddedToMemory(count );
        }
        rule.setMoreToBuild(false);
        return it;
    }



    private int addCountToIndex(byte indexType, Iterator it, int count) {
        Map.Entry pair = (Map.Entry)it.next();
        ArrayList<Triple> list = (ArrayList<Triple>) pair.getValue();
        for (Triple triple:list) {
            this.addToIndex(indexType , triple);
            count++;
        }
        return count;
    }
}
