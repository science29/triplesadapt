package optimizer;

import QueryStuff.Query;
import QueryStuff.QueryWorkersPool;
import index.Dictionary;
import index.IndexType;
import index.IndexesPool;
import index.MyHashMap;
import triple.ResultTriple;
import triple.Triple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class EvictQueueElement {

    Query query;
    double benefit;
    byte indexType;
    public boolean isAllocated = false;
    private Iterator<Map.Entry<Integer,char[]>> previousSourceIter;


    public EvictQueueElement(Query query, double benefit, byte indexType) {
        this.query = query;
        this.benefit = benefit;
        this.indexType = indexType;
    }


    public void setBenefit(float benefit){
        this.benefit = benefit;
    }


    public void deAllocate(QueryWorkersPool queryWorkersPool , IndexesPool indexesPool) {
        //evaluate then remove
        if(query == null){

            return;
        }
        query.setDoneListener(new Query.QueryDoneListener() {
            @Override
            public void done() {
                ResultTriple resultTriple = query.getTripleResults();
                while(resultTriple != null) {
                    Triple triple = resultTriple.getTriple();
                    MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(indexType);
                    index.removeMatchingTriples(triple, indexType);
                    resultTriple = resultTriple.getDown();
                }

            }
        });
        queryWorkersPool.addQuery(query);
    }



    MyHashMap<Integer, ArrayList<Triple>> index;



    public void allocate(QueryWorkersPool queryWorkersPool , IndexesPool indexesPool , Dictionary dictionary , int step) {

        int count = 0;
        if(query == null){
            if(previousSourceIter == null) {
                previousSourceIter = dictionary.getIterator();
                index = indexesPool.getIndex(indexType);
            }
            while (previousSourceIter.hasNext()){
                Map.Entry<Integer, char[]> entery = previousSourceIter.next();
                Integer v = entery.getKey();
               if(!index.containsKey(v)){
                   ArrayList<Triple> list = indexesPool.getTriples(v);
                   index.putAndSort(v , list , IndexesPool.getFirstIndex(indexType) ,
                           IndexesPool.getSecondIndex(indexType));
                   count++;
                   if(count > step)
                       return;
               }
            }
            return;
        }

        query.setDoneListener(new Query.QueryDoneListener() {
            @Override
            public void done() {
                ResultTriple resultTriple = query.getTripleResults();
                while(resultTriple != null) {
                    Triple triple = resultTriple.getTriple();
                    MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(indexType);
                    index.addTriples(triple, indexType , dictionary);
                    resultTriple = resultTriple.getDown();
                }

            }
        });
        queryWorkersPool.addQuery(query);

    }



}
