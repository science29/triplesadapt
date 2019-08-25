package QueryStuff;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueryWorkersPool {


    private BlockingQueue<Query> sharedWorkQueue = new ArrayBlockingQueue(10000);
    private ArrayList<Worker> workers = new ArrayList<>();

    public void addQuery(Query query){
        try {
            sharedWorkQueue.put(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class Worker extends  Thread{

        @Override
        public void run(){
            try {
                Query query = sharedWorkQueue.take();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
