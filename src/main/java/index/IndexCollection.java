package index;

import triple.Triple;
import triple.TriplePattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IndexCollection {


    private HashMap<Integer, MyHashMap<Long, ArrayList<Triple>>> longIndeciesMap;
    private HashMap<Integer, MyHashMap<String, ArrayList<Triple>>> stringIndeciesMap;

    public IndexCollection() {
        longIndeciesMap = new HashMap();
        stringIndeciesMap = new HashMap();
    }

    public void addIndex(MyHashMap<Long, ArrayList<Triple>> index, IndexType indexType) {
        longIndeciesMap.put(indexType.getCode(), index);
    }

    public void addIndexStringKey(MyHashMap<String, ArrayList<Triple>> index, IndexType indexType) {
        stringIndeciesMap.put(indexType.getCode(), index);
    }


    //TODO solve the problem of string and long key of indexes
    public MyHashMap<Long, ArrayList<Triple>> getBestIndex(TriplePattern triplePattern) {
        IndexType indexType = new IndexType(triplePattern);
        int bestCode = indexType.getCode();
        MyHashMap<Long, ArrayList<Triple>> bestIndex = longIndeciesMap.get(bestCode);
        if (bestIndex != null)
            return bestIndex;

        switch (bestCode) {
            case 111:
                bestIndex = longIndeciesMap.get(110);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(11);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(101);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(100);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(1);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(10);
                return bestIndex;
            case 110:
                bestIndex = longIndeciesMap.get(100);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(10);
                return bestIndex;
            case 11:
                bestIndex = longIndeciesMap.get(1);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(10);
                return bestIndex;
            case 101:
                bestIndex = longIndeciesMap.get(100);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = longIndeciesMap.get(1);
                return bestIndex;

        }
        return null;
    }


    public void printStat(){

        Iterator itmap = longIndeciesMap.entrySet().iterator();
        while (itmap.hasNext()) {
            Map.Entry pair = (Map.Entry) itmap.next();
            MyHashMap index = (MyHashMap) pair.getValue();
            printIndexStat(index);
        }

        itmap = stringIndeciesMap.entrySet().iterator();
        while (itmap.hasNext()) {
            Map.Entry pair = (Map.Entry) itmap.next();
            MyHashMap index = (MyHashMap) pair.getValue();
            printIndexStat(index);
        }
    }


    public static void printIndexStat(MyHashMap index){
        int elementCount = index.size();
        String name = index.getFileName();
        double sizeGB = index.getSizeGB();
        System.out.println("index:"+name+", "+sizeGB+" GB, Mem:"+index.getMemorySize()+" GB,"+elementCount+" Items");
    }



}
