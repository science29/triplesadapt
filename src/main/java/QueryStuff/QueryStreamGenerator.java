package QueryStuff;

import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import index.MyHashMap;
import optimizer.HeatQuery;
import optimizer.Optimizer2;
import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class QueryStreamGenerator extends Thread {


    private final QueryWorkersPool queryWorkersPool;
    private final IndexesPool indexPool;
    private int averageLength;
    private int maxLength;
    private int meanFrequency;
    private int quality;
    private long period;
    private Transporter transporter;
    private final Dictionary reverseDictionary;
    private final MyHashMap<Integer, ArrayList<Triple>> OPS;
    private ArrayList<String> newHeaveyQueries;
    private HeatQuery heatMap;
    private Optimizer2 optimizer;
    private int queryNoSequence = 84734;


    public QueryStreamGenerator(int averageLength, int maxLength, int meanFrequency, int quality, int period, Transporter transporter,
                                QueryWorkersPool queryWorkersPool, Dictionary reverseDictionary , IndexesPool indexesPool, HeatQuery heatMap , Optimizer2 optimizer2) {
        this.averageLength = averageLength;
        this.maxLength = maxLength;
        this.meanFrequency = meanFrequency;
        this.quality = quality;
        this.period = period;
        this.transporter = transporter;
        this.queryWorkersPool = queryWorkersPool;
        this.reverseDictionary = reverseDictionary;
        this.OPS = indexesPool.getIndex(IndexesPool.OPs);
        this.indexPool = indexesPool;
        this.heatMap = heatMap;
        this.optimizer = optimizer2;
    }
    

    @Override
    public void run(){
       String query = generartNext();
        int queryNo = queryWorkersPool.addSingleQuery(query);
        transporter.sendQuery(query , queryNo);
        try {
            sleep((long) getNextPeriod());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    Random sleepPeroidRan = new Random();

    //TODO to be fixed
    private double getNextPeriod() {
        return sleepPeroidRan.nextGaussian()*period+period;
    }

    private String generartNext() {
        int nextLength = getNextLength();
        Random qualityRandom = new Random();
        int ch = qualityRandom.nextInt(100);
        String query;
        if(ch > 50)
            query = getNewRandomQuery(nextLength);
        else
            query = getSeenQuery(nextLength);
        if(query == null)
            query = getNewRandomQuery(nextLength);
        return query;
    }



    private String getSeenQuery(int length) {
        TriplePattern2 first = heatMap.getNextStoredQuery(length , indexPool);
        ArrayList<Triple> list = new ArrayList<>();
        while (first != null){
            list.add(new Triple(first.getTriples()[0] , first.getTriples()[1]  , first.getTriples()[2] ));
            first = first.getRights().get(0);
        }
        //Query query = new Query(reverseDictionary , first , indexPool , transporter , queryNoSequence++ , optimizer);
        String query = QueryGenrator.translateDeepQuery(list , reverseDictionary);
        return query;

        /*int ch = leftRight.nextInt(2);
        HashMap<Integer, HeatQuery.HeatElement> map;
        if(ch ==0 )
           map = queryElement.left;
        else
            map = queryElement.right;*/


    }


    private String getNewRandomQuery(int nextLength){
        if(newHeaveyQueries == null || newHeaveyQueries.size() == 0)
            newHeaveyQueries = QueryGenrator.buildFastHeavyQueryZero(reverseDictionary , OPS , 1 , nextLength);
        return newHeaveyQueries.remove(0);
    }

    Random random = new Random();

    private int getNextLength() {
        float sd = maxLength - averageLength;
        int l = (int)Math.round(random.nextGaussian()*sd+averageLength);
        if( l < 0)
            l = 0;
        if(l > maxLength)
            l = maxLength;
        return l;
    }

}
