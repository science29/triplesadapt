package QueryStuff;

import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;

public class InterExecutersPool {

    private final ArrayBlockingQueue sharedBlockingQueue;
    private ArrayList <InterQueryExecuter> threadPool = new ArrayList();
    private boolean[] threadsStatus;

    public void setFinalListener(TriplePattern2.ExecuterCompleteListener finalListener) {
        this.finalListener = finalListener;
    }

    public TriplePattern2.ExecuterCompleteListener finalListener;

    public InterExecutersPool(int threadCount) {
        threadsStatus = new boolean[threadCount];
        sharedBlockingQueue = new ArrayBlockingQueue(10000);
        createThreadPool(threadCount);
    }



    private void createThreadPool(int count){
        for(int i = 0 ; i < count ; i++){
            InterQueryExecuter queryExecuter = new InterQueryExecuter(sharedBlockingQueue , i , this);
            threadPool.add(queryExecuter);
            queryExecuter.start();
        }
    }

    private synchronized void sort(){
        Collections.sort(threadPool, new Comparator<InterQueryExecuter>() {
            // @Override
            public int compare(InterQueryExecuter lhs, InterQueryExecuter rhs) {
                if(lhs.getWorkingSize() < rhs.getWorkingSize())
                    return -1;
                if(lhs.getWorkingSize() > rhs.getWorkingSize())
                    return 1;
                return 0;
            }
        });
    }


    public ArrayList<InterQueryExecuter> getThreadPool(int threadCount) {
        sort();//TODO optimize
        return threadPool;
    }

    public void stop(){
        for(int i = 0 ; i < threadPool.size() ; i++){
            threadPool.get(i).stopWorker();
        }
    }


    public void setWorking(boolean status, int threadId) {
        threadsStatus[threadId] = status;
    }

    public int getAvailableThreadCount(){
        int cnt = 0;
        for(int i = 0; i < threadsStatus.length ; i++){
            if(!threadsStatus[i])
                cnt++;
        }
        return cnt;
    }
}
