package QueryStuff;

import index.Dictionary;
import sun.jvm.hotspot.opto.HaltNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueryWorkersPool {


    private static final int NO_OF_THREADS = 2;
    private ArrayList<BlockingQueue<Query> > sharedWorkQueues = new ArrayList<>();
    private ArrayList<Worker> workers = new ArrayList<>();
    private final Dictionary revereseDictionary;

    private HashMap<Integer , Query> pendingQuery = new HashMap<>();

    public QueryWorkersPool(Dictionary revereseDictionary){
        this.revereseDictionary = revereseDictionary;
        for(int i = 0 ; i < NO_OF_THREADS ; i++){
            sharedWorkQueues.add(new ArrayBlockingQueue<>(1000));
            workers.add(new Worker(i));
        }
    }

    public void addQuery(Query query ){
        try {
            int min_size = sharedWorkQueues.get(0).size();
            int min_index = 0;
            for(int i = 0 ; i < NO_OF_THREADS ; i++){
                if(sharedWorkQueues.get(i).size() == 0)
                    sharedWorkQueues.get(i).put(query);
                if(sharedWorkQueues.size() < min_size) {
                    min_size = sharedWorkQueues.size();
                    min_index = i;
                }
            }
            sharedWorkQueues.get(min_index).put(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class Worker extends Thread{

        private final int threadID;

        private Worker(int threadID) {
            this.threadID = threadID;
        }

        @Override
        public void run(){
            try {
                Query query = sharedWorkQueues.get(threadID).take();
                query.setBorderChangeListener();
                if(!query.isPendingBorder()) {
                    query.findQueryAnswer();
                    if (!query.isPendingBorder())
                        query.printAnswers(revereseDictionary, false);
                    else
                        pendingQuery.put(query.ID, query);
                }else {
                    query.borderEvaluation();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
