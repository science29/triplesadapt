package index;

import optimizer.EngineRotater;
import optimizer.EngineRotater2;
import start.MainUinAdapt;
import triple.Triple;
import triple.TriplePattern;
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

    public final static byte SPo_r = 13;


    public final static byte PSo_aggregate = 14;

    public final static byte SP_o = 15;
    public final static byte OP_s = 16;

    public final static byte PPx = 17;

    private final Dictionary dictionary;



    HashMap<Byte, MyHashMap<Integer,ArrayList<Triple>>> pool;

    final HashMap<Integer , Integer> selectivity;

    final public HashMap<Integer , Boolean> isBorderMap;

    public IndexesPool(HashMap<Integer , Boolean> isBorderMap, Dictionary dictionary ){
        pool = new HashMap<>();
        this.isBorderMap = isBorderMap;
        this.dictionary = dictionary;
        selectivity = new HashMap<>();
    }

    public MyHashMap<Integer,ArrayList<Triple>> getRanIndex(){
        MyHashMap<Integer,ArrayList<Triple>> indexXX =  pool.entrySet().iterator()
                .next().getValue();
        return indexXX;
    }

    public static int getFirstIndex(byte type) {
        if(type == OPs || type == OSp || type == Osp || type == Ops )
            return 2;
        if(type == SOp || type == SPo || type == Sop || type == Spo )
            return 0;
        return 1;
    }


    public static int getSecondIndex(byte type) {
        if(type == SOp || type == POs )
            return 2;
        if(type == PSo || type == OSp)
            return 0;
        return 1;
    }

    public void addIndex(MyHashMap index , byte type){
        index.poolRefType = (byte)type;
        pool.put(type , index);
    }


    public MyHashMap<Integer , ArrayList<Triple>> getIndex(byte type){
        return pool.get(type);
    }

    public void addIndex(byte type, HashMap<Integer, ArrayList<Triple>> map , String name ) {
        MyHashMap<Integer , ArrayList<Triple>> cov = new MyHashMap<Integer, ArrayList<Triple>>(name , map);
        addIndex(cov , type);
    }


    public boolean isBorder(Triple triple , int index){
        return isBorderMap.containsKey(triple.triples[index]);
    }

    public int getSelectivity(int index){
        return selectivity.get(index);
    }

    public ArrayList<Triple> get(byte optimalIndexType , Integer first , Integer second , TriplePattern2.WithinIndex withinIndex , EngineRotater engineRotater, TriplePattern2 triplePattern ){
        //first try the optimal
        MyHashMap<Integer,ArrayList<Triple>> optimal = pool.get(optimalIndexType);
        ArrayList<Triple> list;
        int sortedIndex = getSortedIndex(optimalIndexType);
        if(second == -1)
           list =  optimal.get(first);
        else
            list = optimal.get(first , second ,sortedIndex ,withinIndex);
        if(list != null) {
            if(engineRotater != null)
                engineRotater.informOptimalIndexUsage(optimal , first , second  ,triplePattern , withinIndex.potentailFilterCost);
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
            if(engineRotater != null)
                engineRotater.informSubOptIndexUsage(index,optimal, first , second , EngineRotater.FULL_DATA_COST);
            return null;
        }
        list = index.get(second , first ,sortedIndex ,withinIndex);
        int benefit = (int)(withinIndex.cost - Math.log(getSelectivity(first))) ;
        if(engineRotater != null)
            engineRotater.informSubOptIndexUsage(index,optimal, first , second , benefit);
        if(list != null)
            return list;
        return null;
    }


    public ArrayList<Triple> get(Integer first , Integer second , int firstIndex, int secondIndex ,
                                 EngineRotater2.OperationCost operationCost){
        byte optimalIndex = getOptimalIndex(firstIndex , secondIndex);
        byte nearestIndex = getNearestIndex(optimalIndex , true);
        if(nearestIndex == -1) {
            if(operationCost != null)
                operationCost.addNoSuitableIndexCost();
            return null;
        }

        if(second == null)
            return  getIndex(nearestIndex).get(first);

        if(getFirstIndex(nearestIndex)  == getFirstIndex(optimalIndex)){
            if(getSortedIndex(nearestIndex) == getSortedIndex(optimalIndex))
                return getIndex(optimalIndex).get(first ,second,getSortedIndex(nearestIndex),null);
            else{
                //use the first as key then filter on the second
                return filter(getIndex(nearestIndex).get(first) , second , secondIndex , operationCost);
            }

        }else if(getFirstIndex(nearestIndex) == getSortedIndex(optimalIndex)){ // it is a reverse
            return getIndex(nearestIndex).get(second ,first, firstIndex,null);
        }


        //use the second as first key then filter on the first
        return filter(getIndex(nearestIndex).get(second) , first , firstIndex , operationCost);

    }

    private ArrayList<Triple> filter(ArrayList<Triple> triples, Integer filterItem, int filterIndex ,
                                     EngineRotater2.OperationCost operationCost) {
        ArrayList<Triple> res = new ArrayList<>();
        for(int i = 0 ; i < triples.size() ; i++){
            if(triples.get(i).triples[filterIndex] == filterItem)
                res.add(triples.get(i));
        }
        if(operationCost != null){
            operationCost.cost += triples.size() + res.size();
        }
        return res;
    }


    private int getSortedIndex(byte optimalIndexType) {
        switch (optimalIndexType){
            case OPs: return 1;
            case SPo: return 1;
            case OSp: return 0;
            case SOp: return 2;
            case PSo: return 0;
            case POs: return 2;
            case SPo_r: return 1;
        }
        return -1;
    }


    private int getHashedIndex(byte optimalIndexType) {
        switch (optimalIndexType){
            case OPs: return 2;
            case SPo: return 0;
            case OSp: return 2;
            case SOp: return 0;
            case PSo: return 1;
            case POs: return 1;
            case SPo_r: return 0;
        }
        return -1;
    }

    private int getLastIndex(byte indexType) {
        switch (indexType){
            case OPs: return 0;
            case SPo: return 2;
            case OSp: return 1;
            case SOp: return 1;
            case PSo: return 2;
            case POs: return 0;
            case SPo_r: return 2;
        }
        return -1;
    }


    public void addToIndex(byte indexType , byte aggegateIndex, Triple tripleObj, boolean aggregate) {
        MyHashMap<Integer, ArrayList<Triple>> index = pool.get(indexType);
        if(index == null){
            index = new MyHashMap<>(indexType+"");
            pool.put(indexType , index);
        }
        int key = getHashedIndex(indexType);
        if (index.containsKey(tripleObj.triples[key])) {

        }
    }



    public void addToIndex(byte indexType, Triple tripleObj) {
        MyHashMap<Integer, ArrayList<Triple>> index = pool.get(indexType);
        if(index == null){
            index = new MyHashMap<>(indexType+"");
            pool.put(indexType , index);
        }

        int key = getHashedIndex(indexType);
       // int sortedKey = getSortedIndex(indexType);

        if (index.containsKey(tripleObj.triples[key])) {
            ArrayList<Triple> list  = index.get(tripleObj.triples[key]);
            list.add(tripleObj);
            //binaryAdd(list, key ,tripleObj);
        } else {
            ArrayList<Triple> list = new ArrayList();
            list.add(tripleObj);
            Integer codeObj = dictionary.get(dictionary.get(tripleObj.triples[key]));
            index.put(codeObj, list);
        }
    }



    private static void binaryAdd(ArrayList<Triple> arr, int index, Triple toAdd) {

        /*for(int i = 0; i < arr.size() && arr.size() > 1 ; i++){
            if(i == arr.size()-1)
                break;
            if(arr.get(i).triples[index] > arr.get(i+1).triples[index]){
                System.err.println("errrror");
            }
        }*/
        int key = toAdd.triples[index];
        int last = arr.size() - 1;
        int first = 0;
        int mid = (first + last) / 2;
        while (first <= last) {
            if (arr.get(mid).triples[index] < key) {
                first = mid + 1;
            } else if (arr.get(mid).triples[index] == key) {
                // System.out.println("Element is found at index: " + mid);
                int found = arr.get(mid).triples[index];
                int t = mid;
                while (t > 0 && arr.get(t - 1).triples[index] == found) {
                    t--;
                }
                arr.add(mid,toAdd);
                return ;
            } else {
                last = mid - 1;
            }
            mid = (first + last) / 2;
        }
        arr.add(mid,toAdd);
        return ;
    }




    public void addToReplication(Triple triple) {
        addToIndex(SPo_r , triple);
    }


    public void sortAllSortedIndexes(){
        Iterator it = pool.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            Byte indexType = (Byte) pair.getKey();
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

    public Iterator buildIndex(byte startIndexType, byte indexType, Iterator prevoiusIterator, int quantity , EngineRotater.Rule rule) {
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


    public static String getIndexName(byte indexType){
        String indexName = null ;
        switch (indexType){
            case IndexesPool.SPo: indexName ="SPo"; break;
            case IndexesPool.SOp: indexName ="SOp"; break;
            case IndexesPool.POs: indexName ="POs"; break;
            case IndexesPool.PSo: indexName ="PSo"; break;
            case IndexesPool.OPs: indexName ="OPs"; break;
            case IndexesPool.OSp: indexName ="OSp"; break;

            case IndexesPool.SP_o: indexName ="SP_o"; break;
            case IndexesPool.OP_s: indexName ="OP_s"; break;
            case IndexesPool.PPx:indexName ="PPx"; break;

        }
        return indexName;
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

    public ArrayList<Triple> getTriples(Integer v) {
        Iterator<Map.Entry<Byte, MyHashMap<Integer, ArrayList<Triple>>>> iter = pool.entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry<Byte, MyHashMap<Integer, ArrayList<Triple>>> entery = iter.next();
            MyHashMap<Integer, ArrayList<Triple>> index = entery.getValue();
            if(index.containsKey(v))
                return index.get(v);
        }
        return null;
    }

   /// OPs , SOp ,PSo      SPo,OSp,POs

    public byte getNearestIndex(byte indexType , boolean allowReverse){
        MyHashMap<Integer, ArrayList<Triple>> returnIndex;
        switch (indexType){
            case OPs:
                returnIndex = pool.get(OPs);
                if(returnIndex != null)
                    return OPs;
                if(allowReverse){
                    returnIndex = pool.get(POs);
                    if(returnIndex != null)
                        return POs;
                }
                returnIndex = pool.get(OSp);
                if(returnIndex != null)
                    return OSp;
                returnIndex = pool.get(Osp);
                if(returnIndex != null)
                    return Osp;
                returnIndex = pool.get(Ops);
                if(returnIndex != null)
                    return Ops;
                returnIndex = pool.get(POs);
                if(returnIndex != null)
                    return POs;
                returnIndex = pool.get(PSo);
                if(returnIndex != null)
                    return PSo;
                return -1;

            case OSp:
                returnIndex = pool.get(OSp);
                if(returnIndex != null)
                    return OSp;
                if(allowReverse){
                    returnIndex = pool.get(SOp);
                    if(returnIndex != null)
                        return SOp;
                }
                returnIndex = pool.get(OPs);
                if(returnIndex != null)
                    return OPs;

                returnIndex = pool.get(SOp);
                if(returnIndex != null)
                    return SOp;
                returnIndex = pool.get(SPo);
                if(returnIndex != null)
                    return SPo;
                return -1;

                case SPo:
                    returnIndex = pool.get(SPo);
                    if(returnIndex != null)
                        return SPo;
                    if(allowReverse){
                        returnIndex = pool.get(PSo);
                        if(returnIndex != null)
                            return PSo;
                    }
                    returnIndex = pool.get(SOp);
                    if(returnIndex != null)
                        return SOp;
                    returnIndex = pool.get(Spo);
                    if(returnIndex != null)
                        return Spo;
                    returnIndex = pool.get(Sop);
                    if(returnIndex != null)
                        return Sop;
                    returnIndex = pool.get(PSo);
                    if(returnIndex != null)
                        return PSo;
                    returnIndex = pool.get(PSo);
                    if(returnIndex != null)
                        return PSo;
                    return -1;

            case SOp:
                returnIndex = pool.get(SOp);
                if(returnIndex != null)
                    return SOp;
                if(allowReverse){
                    returnIndex = pool.get(OSp);
                    if(returnIndex != null)
                        return OSp;
                }
                returnIndex = pool.get(SPo);
                if(returnIndex != null)
                    return SPo;

                returnIndex = pool.get(POs);
                if(returnIndex != null)
                    return POs;
                returnIndex = pool.get(PSo);
                if(returnIndex != null)
                    return PSo;
                return -1;


            case POs:
                returnIndex = pool.get(POs);
                if(returnIndex != null)
                    return POs;
                if(allowReverse){
                    returnIndex = pool.get(OPs);
                    if(returnIndex != null)
                        return OPs;
                }
                returnIndex = pool.get(PSo);
                if(returnIndex != null)
                    return PSo;

                returnIndex = pool.get(OSp);
                if(returnIndex != null)
                    return OSp;
                returnIndex = pool.get(OPs);
                if(returnIndex != null)
                    return OPs;
                return -1;


            case PSo:
                returnIndex = pool.get(PSo);
                if(returnIndex != null)
                    return PSo;
                if(allowReverse){
                    returnIndex = pool.get(SPo);
                    if(returnIndex != null)
                        return SPo;
                }
                returnIndex = pool.get(POs);
                if(returnIndex != null)
                    return PSo;

                returnIndex = pool.get(SPo);
                if(returnIndex != null)
                    return OSp;
                returnIndex = pool.get(OPs);
                if(returnIndex != null)
                    return OPs;
                return -1;



        }
        return -1;
    }


    public byte getOptimalIndex(int firstIndex, int secondIndex){
        switch (firstIndex){
            case 0:
                switch (secondIndex) {
                    case -1:
                        return Spo;
                    case 1:
                        return SPo;
                    case 2:
                        return SOp;
                }


            case 1:
                switch (secondIndex) {
                    case -1:
                        return Pso;
                    case 0:
                        return PSo;
                    case 2:
                        return POs;
                }


            case 2:
                switch (secondIndex) {
                    case -1:
                        return Ops;
                    case 0:
                        return OSp;
                    case 1:
                        return OPs;
                }

        }
        return -1;
    }



}
