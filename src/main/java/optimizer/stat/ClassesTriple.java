package optimizer.stat;

import index.MyHashMap;
import triple.Triple;

import java.util.ArrayList;

public class ClassesTriple {

    Triple tripleAbstract;

    int count;

    int totalSClass;
    int totalOClass;

    ArrayList<Triple> triples;

    ArrayList<Triple> sSorted;
    ArrayList<Triple> oSorted;

    ClassesTriple(Triple triple){
        this.tripleAbstract = triple;
        count = 0;
        this.totalOClass = 0;
        this.totalSClass = 0;
        triples = new ArrayList<>();
    }


    public void addTriple(Triple tripleOrg) {
        triples.add(tripleOrg);
    }

    public void sortOnS(){
        sSorted = triples;
        oSorted = null;
        MyHashMap.sortArray(triples,0,2);
    }


    public void sortOnO(){
        oSorted = triples;
        sSorted = null;
        MyHashMap.sortArray(triples,2,0);
    }

    public void dupSortOnBoth(){
        oSorted = new ArrayList<>(triples);
        sSorted = triples;
        MyHashMap.sortArray(sSorted,0,2);
        MyHashMap.sortArray(oSorted,2,0);
    }

    public ArrayList<Triple> getSTriples(){
        return sSorted;
    }

    public ArrayList<Triple> getOTriples(){
        return oSorted;
    }
}
