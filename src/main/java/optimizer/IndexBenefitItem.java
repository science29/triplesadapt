package optimizer;

import index.MyHashMap;
import triple.Triple;

import java.util.ArrayList;

public class IndexBenefitItem {

    public final MyHashMap<Integer , ArrayList<Triple>>  index;
    private int usage;



    private int currentInMemoryCount;

    public IndexBenefitItem(MyHashMap<Integer , ArrayList<Triple>> index) {
        this.index = index;
        usage = 0;
        currentInMemoryCount = index.size();
    }

    public void increaseUsage(int usage){
        this.usage += usage;
    }


    public int getUsage() {
        return usage;
    }

    public int getCurrentInMemoryCount() {
        return currentInMemoryCount;
    }




}
