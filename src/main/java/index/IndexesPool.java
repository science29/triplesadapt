package index;

import optimizer.Optimiser;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;

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




    HashMap<Integer, MyHashMap<Integer,ArrayList<Triple>>> pool;

    final HashMap<Integer , Integer> selectivity;

    final HashMap<Integer , Boolean> isBorder;

    public IndexesPool(HashMap<Integer , Boolean> isBorder){
        pool = new HashMap<>();
        this.isBorder = isBorder;
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

    public ArrayList<Triple> get(int optimalIndexType , int first , int second , TriplePattern2.WithinIndex withinIndex , Optimiser optimiser ){
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
        if(index == null) {
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
}
