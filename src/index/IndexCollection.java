package index;

import triple.Triple;
import triple.TriplePattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IndexCollection {


    private HashMap<Integer, MyHashMap<String, ArrayList<Triple>>> indeciesMap;

    public IndexCollection() {
        indeciesMap = new HashMap<>();
    }

    public void addIndex(MyHashMap<String, ArrayList<Triple>> index, IndexType indexType) {
        indeciesMap.put(indexType.getCode(), index);
    }

    public MyHashMap<String, ArrayList<Triple>> getBestIndex(TriplePattern triplePattern) {
        IndexType indexType = new IndexType(triplePattern);
        int bestCode = indexType.getCode();
        MyHashMap<String, ArrayList<Triple>> bestIndex = indeciesMap.get(bestCode);
        if (bestIndex != null)
            return bestIndex;

        switch (bestCode) {
            case 111:
                bestIndex = indeciesMap.get(110);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(11);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(101);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(100);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(1);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(10);
                return bestIndex;
            case 110:
                bestIndex = indeciesMap.get(100);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(10);
                return bestIndex;
            case 11:
                bestIndex = indeciesMap.get(1);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(10);
                return bestIndex;
            case 101:
                bestIndex = indeciesMap.get(100);
                if (bestIndex != null)
                    return bestIndex;
                bestIndex = indeciesMap.get(1);
                return bestIndex;

        }
        return null;
    }


    public void printStat(){

        Iterator itmap = indeciesMap.entrySet().iterator();
        while (itmap.hasNext()) {
            Map.Entry pair = (Map.Entry) itmap.next();
            MyHashMap index = (MyHashMap) pair.getValue();
            int elementCount = index.size();
            String name = index.getFileName();
            double sizeGB = index.getSizeGB();
            System.out.println("index:"+name+", "+sizeGB+" GB,"+elementCount+" Items");
        }
    }


}
