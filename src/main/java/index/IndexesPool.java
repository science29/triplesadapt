package index;

import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;

public class IndexesPool {

    public final static int Spo = 0;
    public final static int SPo = 2;
    public final static int Sop = 3;
    public final static int SOp = 4;

    public final static int Pso = 5;
    public final static int PSo = 6;
    public final static int Pos = 7;
    public final static int POs = 8;

    public final static int Osp = 9;
    public final static int OSp = 10;
    public final static int Ops = 11;
    public final static int OPs = 12;


    HashMap<Integer, MyHashMap<Integer,ArrayList<Triple>>> pool;

    final HashMap<Integer , Boolean> isBorder;

    public IndexesPool(HashMap<Integer , Boolean> isBorder){
        pool = new HashMap<>();
        this.isBorder = isBorder;
    }

    public void addIndex(MyHashMap index , int type){
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



    public ArrayList<Triple> get(int optimalIndexType , int first , int second , TriplePattern2.WithinIndex withinIndex){
        //first try the optimal
        MyHashMap<Integer,ArrayList<Triple>> optimal = pool.get(optimalIndexType);
        ArrayList<Triple> list;
        int sortedIndex = getSortedIndex(optimalIndexType);
        if(second == -1)
           list =  optimal.get(first);
        else
            list = optimal.get(first , second ,sortedIndex ,withinIndex);
        if(list != null)
            return list;

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
        if(index == null)
            return null;
        list = index.get(second , first ,sortedIndex ,withinIndex);
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
    }
}
