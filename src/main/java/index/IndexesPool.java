package index;

import triple.Triple;

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
    public final static int Opo = 11;
    public final static int OPo = 12;

    HashMap<Integer , MyHashMap> pool ;

    public IndexesPool(){
        pool = new HashMap<Integer, MyHashMap>();
    }

    public void addIndex(MyHashMap index , int type){
        pool.put(type , index);
    }


    public HashMap getIndex(int type){
        return pool.get(type);
    }

    public void addIndex(int type, HashMap<Long, ArrayList<Triple>> map , String name ) {
        MyHashMap<Long , ArrayList<Triple>> cov = new MyHashMap<Long, ArrayList<Triple>>(name , map);
        addIndex(cov , type);
    }
}
