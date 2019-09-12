package QueryStuff;

import distiributed.SendItem;
import distiributed.Transporter;
import index.Dictionary;
import index.IndexesPool;
import optimizer.Threading;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueryWorkersPool {


    private static final int NO_OF_THREADS = 2;
    private static final int INTER_EXECUTER_THREAD_COUNT = 7;
   // private ArrayList<BlockingQueue<Query>> sharedWorkQueues = new ArrayList<>();
    private BlockingQueue<Query> sharedWorkQueue = new ArrayBlockingQueue<>(1000);
    private ArrayList<Worker> workers = new ArrayList<>();
    private final Dictionary dictionary;

    private HashMap<Integer, Query> pendingQuery = new HashMap<>();
    private final Transporter transporter;
    private final IndexesPool indexPool;
    private final InterExecutersPool interExecutersPool;


    private int sessionID = -1;
    private long sesstionStartTime;

    public QueryWorkersPool(Dictionary dictionary, Transporter transporter, IndexesPool indexesPool) {
        this.dictionary = dictionary;
        this.transporter = transporter;
        this.indexPool = indexesPool;
        for (int i = 0; i < NO_OF_THREADS; i++) {
          //  sharedWorkQueues.add(new ArrayBlockingQueue<>(1000));
            Worker worker = new Worker(i, this);
            worker.start();
            workers.add(worker);
        }

        interExecutersPool = new InterExecutersPool(INTER_EXECUTER_THREAD_COUNT);

    }

    public synchronized void moveFormPendingToWorking(int queryID) {
        Query query = pendingQuery.get(queryID);
        if (query != null)
            addQuery(query);
    }


    public synchronized void addQuery(Query query) {
        try {
            sharedWorkQueue.put(query);
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

    public int addManyQueries(String query , boolean last ) {
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
    }

    public int addQuery(String query) {
        Query spQuery = new Query(dictionary, query, indexPool, transporter);
        addQuery(spQuery);
        return spQuery.ID;
    }

    public void addQuery(String query, int queryNo) {
        Query spQuery = new Query(dictionary, query, indexPool, transporter);
        spQuery.ID = queryNo;
        addQuery(spQuery);
    }

    private void sessionDone() {
        long endTime = System.nanoTime();
        long time = (endTime - sesstionStartTime) / 1000000;
        System.out.println("file run complete in:"+time + "ms");
        sessionID = -1;
    }


    private class Worker extends Thread {

        private final int threadID;
        private final QueryWorkersPool queryWorkersPool;
        private boolean stop;

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
                    Query query = sharedWorkQueue.take();
                    if (stop)
                        return;
          /*      if(pendingQuery.containsKey(query))
                    pending = true;*/
                    //  query.setBorderChangeListener();
                    if (!query.isPendingBorder()) {
                        int numberOfThreads = Threading.getOptimalQueryThread(sharedWorkQueue.size(), interExecutersPool.getAvailableThreadCount());
                        query.findQueryAnswer(interExecutersPool, queryWorkersPool, numberOfThreads, new TriplePattern2.ExecuterCompleteListener() {
                            @Override
                            public void onComplete() {
                                if (!query.isPendingBorder()) {
                                    if(sessionID == query.ID){
                                        sessionDone();
                                        return;
                                    }
                                    query.printAnswers(dictionary);
                                } else {
                                    pendingQuery.put(query.ID, query);
                                    transporter.receive(query);
                                    //transporter.sendToAll(new SendItem(query.ID , query.triplePatterns2.get(0).getTriples() , query.triplePatterns2.get(0).headResultTriple));
                                    transporter.sendToAll(query.getToSendItem());
                                }
                            }
                        });
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

}
