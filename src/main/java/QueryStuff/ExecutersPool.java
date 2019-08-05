package QueryStuff;

import triple.TriplePattern2;

import java.util.ArrayList;

public class ExecutersPool {

    private ArrayList <QueryExecuter> threadPool = new ArrayList();

    public void setFinalListener(TriplePattern2.ExecuterCompleteListener finalListener) {
        this.finalListener = finalListener;
    }

    public TriplePattern2.ExecuterCompleteListener finalListener;

    public ExecutersPool(int threadCount) {
        this.threadCount = threadCount;
        createThreadPool(threadCount);
    }

    private int threadCount ;

    private void createThreadPool(int count){
        for(int i = 0 ; i < count ; i++){
            QueryExecuter queryExecuter = new QueryExecuter();
            threadPool.add(queryExecuter);
            queryExecuter.start();
        }
    }


    public ArrayList<QueryExecuter> getThreadPool() {
        return threadPool;
    }

    public void stop(){
        for(int i = 0 ; i < threadPool.size() ; i++){
            threadPool.get(i).stopWorker();
        }
    }



}
