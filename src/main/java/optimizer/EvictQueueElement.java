package optimizer;

import QueryStuff.Query;
import QueryStuff.QueryWorkersPool;
import distiributed.SendItem;
import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import index.MyHashMap;
import optimizer.Replication.Replication;
import triple.ResultTriple;
import triple.Triple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class EvictQueueElement {

    SourceSelection sourceSelection;
    double benefit;
    byte indexType;
    public boolean isAllocated = false;
    private Iterator<Map.Entry<Integer,char[]>> previousSourceIter;
    private Iterator<Map.Entry<Integer, ArrayList<Triple>>> previousDestIter;

   // final private int sourceNodeNumber ;

    public static int LOCAL = -1;
    private final Transporter transporter;


    public EvictQueueElement(SourceSelection sourceSelection, double benefit, byte indexType, Transporter transporter) {
        this.sourceSelection = sourceSelection;
        this.benefit = benefit;
        this.indexType = indexType;
       // this.sourceNodeNumber = sourceNodeNumber;
        this.transporter = transporter;
    }


    public void setBenefit(float benefit){
        this.benefit = benefit;
    }


    public void deAllocate(QueryWorkersPool queryWorkersPool , IndexesPool indexesPool , Dictionary dictionary , int step , Replication replication) {
        int count = 0;
        //evaluate then remove
        if(sourceSelection.type == SourceSelection.BORDER_REPLICATION){
            replication.backOneStepReplication();
            return;
        }
        if(sourceSelection.type == SourceSelection.LOCAL_INDEX){
            if(sourceSelection.query == null){
                if(previousDestIter == null) {
                    previousDestIter = indexesPool.getIndex(indexType).entrySet().iterator();
                    index = indexesPool.getIndex(indexType);
                }
                while (previousDestIter.hasNext()){
                    Map.Entry<Integer, ArrayList<Triple>> entery = previousDestIter.next();
                    Integer v = entery.getKey();
                    index.remove(v);
                    count++;
                    if(count > step)
                        return;
                }
                return;
            }
        }
        sourceSelection.query.setDoneListener(new Query.QueryDoneListener() {
            @Override
            public void done() {
                ResultTriple resultTriple = sourceSelection.query.getTripleResults();
                while(resultTriple != null) {
                    Triple triple = resultTriple.getTriple();
                    MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(indexType);
                    index.removeMatchingTriples(triple, indexType);
                    resultTriple = resultTriple.getDown();
                }
            }
        });
        queryWorkersPool.addQuery(sourceSelection.query);


          /*query.setDoneListener(new Query.QueryDoneListener() {
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
        queryWorkersPool.addQuery(query);*/
    }



    MyHashMap<Integer, ArrayList<Triple>> index;



    public void allocate(QueryWorkersPool queryWorkersPool , IndexesPool indexesPool , Dictionary dictionary , int step , Replication replication) {
        //we check if weather its a replication work
        int count = 0;
        if(sourceSelection.type == SourceSelection.BORDER_REPLICATION){
            replication.performOneStepReplication();
            return;
        }
        if(sourceSelection.type == SourceSelection.LOCAL_INDEX){
            //we either get replication or continue doing prevoius index iterator
            if(sourceSelection.query == null){
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
            sourceSelection.query.setDoneListener(new Query.QueryDoneListener() {
                @Override
                public void done() {
                    ResultTriple resultTriple = sourceSelection.query.getTripleResults();
                    addResultTripleToIndex(resultTriple , indexesPool,dictionary);
                }
            });
            queryWorkersPool.addQuery(sourceSelection.query);
        }
    }


    private void addResultTripleToIndex(ResultTriple resultTriple, IndexesPool indexesPool, Dictionary dictionary){
        while(resultTriple != null) {
            Triple triple = resultTriple.getTriple();
            MyHashMap<Integer, ArrayList<Triple>> index = indexesPool.getIndex(indexType);
            index.addTriples(triple, indexType , dictionary);
            resultTriple = resultTriple.getDown();
        }
    }




}
