package index;

import triple.MergeJoinResList;
import triple.MergeJoinResTotal;
import triple.Triple;
import triple.TriplePattern2;

import java.util.HashMap;

public class CacheMergeJoinRes {

    public HashMap<Integer , MergeJoinResTotal> map = new HashMap<>();


    public MergeJoinResTotal get(Triple tripleLeft , Triple tripleRight){

        int key = getKey(tripleLeft, tripleRight);
        return map.get(key);
    }

    public void put( MergeJoinResTotal mergeJoinResTotal ){
        put(mergeJoinResTotal.leftAbstract,mergeJoinResTotal.rightAbstract,mergeJoinResTotal);
    }

    public void put(Triple patternLeft , Triple patternRight, MergeJoinResTotal mergeJoinResTotal ){
        int key = getKey(patternLeft, patternRight);
        if(!map.containsKey(key))
            map.put(key, mergeJoinResTotal);
    }

    private int getKey(Triple tLeft, Triple tRight) {


        int[] tl = tLeft.triples;
        int[] tr = tRight.triples;

        int a1 = func(tl[0],tr[0]);
        int a2 = func(tl[1],tr[1]);
        int a3 = func(tl[2],tr[2]);

        a1 = func(a1,a2);
        a1 = func(a1,a3);

        return a1;
    }


    private int func(int a , int b){
        return  (int)(0.5*(a+b)*(a+b+1))+b;
    }
}
