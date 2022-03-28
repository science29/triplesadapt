package optimizer.stat;

import index.MyHashMap;
import triple.Triple;

import java.util.ArrayList;
import java.util.HashMap;

public class ClassesRel {



    public Integer classID;
    public int count;
    public HashMap<Integer, Integer> predicateStat = new HashMap<>();

    public ClassesRel(Integer classID){
        this.classID = classID;
    }

    public void addPredicateStat(Integer toPredicate){
        Integer cnt = predicateStat.get(toPredicate);
        if(cnt == null)
            cnt = 0;
        cnt++;
        predicateStat.put(toPredicate, cnt);
    }


    public void addPredicateStat(Integer toPredicate, int count){
        predicateStat.put(toPredicate, count);
    }


}
