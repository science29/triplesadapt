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
import java.util.concurrent.ArrayBlockingQueue;

public class QueryStreamGenerator extends Thread {


    private final QueryWorkersPool queryWorkersPool;
    private final IndexesPool indexPool;
    private int averageLength;
    private int maxLength;
    private int meanFrequency;
    private int quality;
    private double period;
    private Transporter transporter;
    private final Dictionary reverseDictionary;
    private final MyHashMap<Integer, ArrayList<Triple>> OPS;
    private ArrayList<String> newHeaveyQueries;
    private HeatQuery heatMap;
    private Optimizer2 optimizer;
    private int queryNoSequence = 84734;
    private boolean stop = false;
    public boolean working;
    private ArrayBlockingQueue<Object> queue ;


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

    public static QueryStreamGenerator getDefault(Transporter transporter, QueryWorkersPool queryWorkersPool, Dictionary reverseDictionary
            , IndexesPool indexesPool, HeatQuery heatMap , Optimizer2 optimizer2) {

        return new QueryStreamGenerator(2,4 , 10 , 50,10,transporter,queryWorkersPool,reverseDictionary,indexesPool,heatMap,optimizer2);
    }


    public void stopGenearaingThread(){
        stop = true;
    }

    @Override
    public void run(){
        while (true) {
            if(stop) {
                working = false;
                try {
                    queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            working = true;
            try {
                String query = generartNext();
                if(query == null)
                    continue;
                int queryNo = queryWorkersPool.addSingleQuery(query);
                transporter.sendQuery(query, queryNo);
            }catch (Exception e){
                e.printStackTrace();
            }
            try {
                sleep((long) getNextPeriod());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    Random sleepPeroidRan = new Random();

    //TODO to be fixed
    private double getNextPeriod() {
        return Math.abs(sleepPeroidRan.nextGaussian()*period+period);
    }

    private String generartNext() {
         int nextLength = getNextLength();
        Random qualityRandom = new Random();
        int ch = qualityRandom.nextInt(100);
        String query;
        if(ch > quality)
            query = getNewRandomQuery(nextLength);
        else
            query = getSeenQuery(nextLength);
        if(query == null)
            query = getNewRandomQuery(nextLength);
        return query;
    }



    private String getSeenQuery(int length) {
        TriplePattern2 first = heatMap.getNextStoredQuery(length , indexPool);
        if(first == null)
            return null;
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
        if(newHeaveyQueries.size() > 0) {
            String q = newHeaveyQueries.get(0);
            newHeaveyQueries.clear();
            return q;
        }
        System.err.print("error:");
        System.out.println("not possible to find query of depth:"+nextLength+" within the data set");
        return null;
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

    public void increaseMaxLength(boolean increase) {
        if(increase)
            maxLength++;
        else
            maxLength--;
        if(maxLength < 0)
            maxLength = 0;
    }

    public void increasePeroid(boolean increase) {
        if(increase)
            period = period*1.1;
        else
            period = period/1.1;
    }

    public void increaseQuality(boolean increase) {
        if(increase)
            quality = quality+10;
        else
            quality = quality-10;
        if(quality > 100)
            quality = 100;
        if(quality < 0)
            quality = 0;

    }

    public double getCurrentQueryQuality() {
        return quality;
    }

    public double getCurrentPeroid() {
        return period;
    }

    public int getMaxLength() {
        return maxLength;
    }


    public void startThread() {
        if(queue == null) {
            queue = new ArrayBlockingQueue<Object>(1);
            start();
        }
        else
            try {
                if(queue.size() == 0)
                    queue.put(new Object());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }
}
