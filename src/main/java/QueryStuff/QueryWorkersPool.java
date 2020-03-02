package QueryStuff;

import distiributed.SendItem;
import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import optimizer.HeatQuery;
import optimizer.Optimiser;
import optimizer.Optimizer2;
import optimizer.Threading;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueryWorkersPool {


    private static final int NO_OF_THREADS = 1;
    private static final int INTER_EXECUTER_THREAD_COUNT = 1;
   // private ArrayList<BlockingQueue<Query>> sharedWorkQueues = new ArrayList<>();
    private BlockingQueue<Query> sharedWorkQueue = new ArrayBlockingQueue<>(1000);
    private ArrayList<Worker> workers = new ArrayList<>();
    private final Dictionary dictionary;

    private HashMap<Integer, Query> pendingQuery = new HashMap<>();
    private final Transporter transporter;
    private final IndexesPool indexPool;
    private final InterExecutersPool interExecutersPool;


    private final boolean[] status;
    private QueryWorkersPool.Session session;
    private long singleStartTime = 0 ;

    private final QueryCache queryCache;

    private final Optimizer2 optimiser;


    public QueryWorkersPool(Dictionary dictionary, Transporter transporter, IndexesPool indexesPool , Optimizer2 optimiser) {
        this.dictionary = dictionary;
        this.transporter = transporter;
        this.indexPool = indexesPool;
        this.status = new boolean[NO_OF_THREADS];
        for (int i = 0; i < NO_OF_THREADS; i++) {
          //  sharedWorkQueues.add(new ArrayBlockingQueue<>(1000));
            Worker worker = new Worker(i, this);
            worker.start();
            workers.add(worker);
        }

        interExecutersPool = new InterExecutersPool(INTER_EXECUTER_THREAD_COUNT);

        queryCache = new QueryCache();
        /*if(optimiser == null)
            this.optimiser = new Optimizer2(this , indexesPool ,dictionary , transporter , border);
        else*/
        this.optimiser = optimiser;
    }



    public synchronized void moveFormPendingToWorking(int queryID) {
        Query query = pendingQuery.get(queryID);
        if (query != null)
            addQuery(query);
    }


    public synchronized void addQuery(Query query) {
        try {
            sharedWorkQueue.put(query);
            optimiser.addQuery(query);
           /* int min_size = sharedWorkQueues.get(0).size();
            int min_index = 0;
            for (int i = 0; i < NO_OF_THREADS; i++) {
                if (sharedWorkQueues.get(i).size() == 0) {
                    sharedWorkQueues.get(i).put(query);
                    return;
                }
                if (sharedWorkQueues.size() < min_size) {
                    min_size = sharedWorkQueues.size();
                    min_index = i;
                }
            }
            sharedWorkQueues.get(min_index).put(query);*/
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void addManyQueries(ArrayList<String> queriesList){
        session = new Session(queriesList.size());
        ArrayList<Integer> queriesNo = Query.getQueiresNumber(queriesList.size()) ;
        transporter.sendManyQueires(queriesList , queriesNo);
        for(int i = 0 ; i < queriesList.size() ; i++){
            try {
                Query spQuery = new Query(dictionary, queriesList.get(i), indexPool, transporter , queriesNo.get(i) , optimiser);
                spQuery.setSilent(true);
                addQuery(spQuery);
               // transporter.sendQuery(queriesList.get(i) , spQuery.ID);
            }catch (Exception e){
                session.lostOne();
            }
        }
    }

    ArrayList<Query> pointless = new ArrayList<>();

    public int addSingleQuery(String query){
        session = null;
        singleStartTime = System.nanoTime();
        Query spQuery = new Query(dictionary, query, indexPool, transporter , optimiser);
        addQuery(spQuery);
        return spQuery.ID;
    }

/*    public int addManyQueries(String query , boolean last ) {
        if(sessionID == -1){
            sesstionStartTime = System.nanoTime();
            this.sessionID = -2;
        }
        Query spQuery = new Query(dictionary, query, indexPool, transporter);
        spQuery.setSilent(true);
        if(last)
            sessionID = spQuery.ID;
        addQuery(spQuery);
        return spQuery.ID;
    }*/

    private Session currentBatch;
    private HashMap<Integer , Session> batchesMap = new HashMap<>();

    public void addQuery(String query, int queryNo , int batchCount , int batchID) {
        Query spQuery = new Query(dictionary, query, indexPool, transporter , optimiser);
        spQuery.ID = queryNo;
        if(batchCount == -1){
            addSingleQuery(query);
            return;
        }

        Session batch;
        if(currentBatch != null && currentBatch.sessionID == batchID)
            batch = currentBatch;
        else
            batch = batchesMap.get(batchID);
        if(batch == null){
            batch = new Session(batchCount);
            currentBatch = batch;
            batchesMap.put(batchID , batch);
        }
        singleStartTime = System.nanoTime();
        spQuery.setBatch(batch);
        addQuery(spQuery);
    }

    private void sessionDone() {
        if(session != null) {
            if(!session.isPrinted())
                session.printSessionTime();
        }
        else{
            System.out.println("single Query done time :"+(System.nanoTime() - singleStartTime)/1000000.0 + " ms ");
        }
    }


    private void batchDone(Session batch) {
        Set<Integer> nos = batch.getDoneQueriesIDs();
        transporter.informDoneQuery(nos);
    }

    public void remoteQueryDone(int queryNo){
        if(session == null || session.queryDone(queryNo))
            sessionDone();
    }

    public void queryDoneLocally(Query query){
        query.done();
        if(query.getBatch() != null){
            if(query.getBatch().queryDone(query.ID))
                batchDone(query.getBatch());
        }else if(session == null || session.queryDone(query.ID))
            sessionDone();
    }



    private void ImWorking(int threadID, boolean working) {
        synchronized(status) {
            System.out.println("thread "+threadID+" working:"+working);
            status[threadID] = working;
        }
    }

    public boolean isFree(){
        synchronized(status) {
            for (int i = 0; i < workers.size(); i++) {
                if (status[i]) {
                    System.out.println("checking "+i+" is still working");
                    return false;
                }
            }
            return true;
        }
    }


    private class Worker extends Thread {

        private final int threadID;
        private final QueryWorkersPool queryWorkersPool;
        private boolean stop;
        private Session mySession;
        private int sessionCount = 0 ;

        private Worker(int threadID, QueryWorkersPool queryWorkersPool) {
            this.threadID = threadID;
            this.queryWorkersPool = queryWorkersPool;
        }



        public void stopWorking() {
            stop = true;
        }

        @Override
        public void run() {
            // boolean pending = false;
            System.out.println("worker qury Thread " + threadID + " is started ..");
            while (!stop) {
                try {
                   // Query query = sharedWorkQueues.get(threadID).take();
                    final Query query = sharedWorkQueue.take();
                    if (stop)
                        return;
                    if(session != null && session.isDone(query.ID))
                        continue;
               //     System.out.println(" query no: "+query.ID+" is started by thread : "+threadID);

          /*      if(pendingQuery.containsKey(query))
                    pending = true;*/
                    //  query.setBorderChangeListener();
                    if (!query.isPendingBorder()) {
                        int numberOfThreads = Threading.getOptimalQueryThread(sharedWorkQueue.size(), interExecutersPool.getAvailableThreadCount());
                        query.findQueryAnswer(interExecutersPool, queryWorkersPool, numberOfThreads, new TriplePattern2.ExecuterCompleteListener() {
                            @Override
                            public void onComplete(Query queryProcessed) {
                                if (!queryProcessed.isPendingBorder()) {
                                   queryDoneLocally(queryProcessed);
                                    /*if(sessionID == query.ID){
                                        sessionDone();
                                        return;
                                    }*/
                    //                System.out.print(" printing query numbered: "+queryProcessed.ID+" at thread "+threadID+" ");
///                                    queryProcessed.printAnswers(dictionary);
                                } else {
                                    pendingQuery.put(queryProcessed.ID, queryProcessed);
                                    transporter.receive(queryProcessed);
                                    //transporter.sendToAll(new SendItem(query.ID , query.triplePatterns2.get(0).getTriples() , query.triplePatterns2.get(0).headResultTriple));
                                    transporter.sendToAll(queryProcessed.getToSendItem());
                                }
                            }
                        } , null);
                        //query.findQueryAnswer(queryWorkersPool);

                    } else {
                        query.borderEvaluation();
                        //TODO print..
                        //TODO how to know if the query is really done??
                        pendingQuery.remove(query);
                        query.printAnswers(dictionary);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (Exception e){
                    e.printStackTrace();
                }

            }
        }
    }



    public class Session{

        int sessionID;
        int queriesCount;
        long startTime;
        long endTime;
        private HashMap<Integer , Boolean> doneQueries = new HashMap<>();
        private boolean printed = false;

        public Session( int queriesCount){
            this.sessionID = new Random().nextInt(1000000);
            this.queriesCount = queriesCount;
            startTime = System.nanoTime();
            endTime = 0;
        }

        public void lostOne() {
            queriesCount--;
        }


        public void printSessionTime(){
            printed = true;
            System.out.println("session done time :"+(endTime-startTime)/1000000.0 + " ms ");
        }

        public synchronized boolean queryDone(int no){
            doneQueries.put(no , true);
            if (doneQueries.size() >= queriesCount){
                endTime = System.nanoTime();
                return true;
            }
            return false;
        }

        public boolean isDone(int id) {
            return doneQueries.containsKey(id);
        }

        public Set<Integer> getDoneQueriesIDs() {
            return doneQueries.keySet();
        }

        public boolean isPrinted() {
            return printed;
        }
    }



}
