package QueryStuff;

import triple.Triple;
import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueryExecuter extends  Thread{
    private BlockingQueue<WorkElement> sharedWorkQueue = new ArrayBlockingQueue(10000);


    private boolean stop;


    public QueryExecuter(){

    }

    @Override
    public void run() {
        while(!stop){
            try {
                WorkElement workElement = sharedWorkQueue.take();
                if(stop)
                    break;
                workElement.triplePattern.startWorkerDeepEvaluationParallel(workElement.list , workElement.from , workElement.to);
                workElement.completeListener.onComplete();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void addWork(TriplePattern2 triplePattern, ArrayList<Triple> list, int from, int to , CompleteListener completeListener) {
        WorkElement workElement = new WorkElement(triplePattern , list ,from , to ,completeListener);
        try {
            sharedWorkQueue.put(workElement);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void stopWorker(){
        stop = true;
        try {
            sharedWorkQueue.put(new WorkElement());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private class WorkElement{
        public ArrayList<Triple> list;
        public int from;
        public int to;
        public TriplePattern2 triplePattern;
        public CompleteListener completeListener;

        public  WorkElement(){
            //dummy constructor to stop the thread..
        }
        public WorkElement(TriplePattern2 triplePattern, ArrayList<Triple> list, int from, int to , CompleteListener completeListener) {
            this.list = list;
            this.triplePattern = triplePattern ;
            this.from = from;
            this.to = to;
            this.completeListener = completeListener;
        }
    }

    public interface CompleteListener{
        void onComplete();
    }
}
