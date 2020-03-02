package optimizer;

import QueryStuff.QueryWorkersPool;
import index.Dictionary;
import index.IndexesPool;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class Evictor2 {

    private static final int ALLOCATE_STEP = 1000000;
    private final Dictionary dictionary;
    private final Optimizer2 optimizer;
    private  EvictorThread evictorThread;
    private QueryWorkersPool queryWorkersPool;
    private IndexesPool indexPool;

    public Evictor2(QueryWorkersPool queryWorkersPool , IndexesPool indexesPool , Dictionary dictionary ,Optimizer2 optimizer2){
        evictorThread = new EvictorThread();
        evictorThread.start();
        this.queryWorkersPool = queryWorkersPool;
        this.indexPool = indexesPool;
        this.dictionary = dictionary ;
        this.optimizer = optimizer2;
    }

    private PriorityQueue<EvictQueueElement> highQBlock = new PriorityQueue<>(new Comparator<EvictQueueElement>() {
        @Override
        public int compare(EvictQueueElement o1, EvictQueueElement o2) {
            if(o1.benefit > o2.benefit)
                return 1;
            if(o1.benefit < o2.benefit)
                return -1;
            return 0;
        }
    });
    private PriorityQueue<EvictQueueElement> lowQBlock = new PriorityQueue<>(new Comparator<EvictQueueElement>() {
        @Override
        public int compare(EvictQueueElement o1, EvictQueueElement o2) {
            if(o1.benefit > o2.benefit)
                return 1;
            if(o1.benefit < o2.benefit)
                return -1;
            return 0;
        }
    });


    public void addToHighQueue(EvictQueueElement evictQueueElement){

        this.highQBlock.add(evictQueueElement);
    }


    public void addToLowQueue(EvictQueueElement evictQueueElement){
        this.lowQBlock.add(evictQueueElement);
    }

    public final static int EVICT_STEP = 100000;

    public void evict(){
        //make space
        double benefitH , benefitL;
        Iterator<EvictQueueElement> hIterator , lIterator;
        int cnt = 0;
        do {
            hIterator = highQBlock.iterator();
            lIterator = lowQBlock.iterator();
            EvictQueueElement h = hIterator.next();
            EvictQueueElement l = lIterator.next();
            evictorThread.addJob(new EvictorJob(h , l));
            benefitH = h.benefit;
            benefitL = l.benefit;
            cnt++;
        } while (hIterator.hasNext() && hIterator.hasNext() && benefitL < benefitH && cnt < EVICT_STEP);


    }

    public void clear() {
        highQBlock.clear();
        lowQBlock.clear();
    }


    public class EvictorThread extends  Thread{

        BlockingQueue<EvictorJob> threadQueue = new ArrayBlockingQueue(1000);
        private boolean stop = false;

        @Override
        public void run(){

            while (!stop){
                try {
                    EvictorJob job = threadQueue.take();
                    job.lowEvictQueueElement.deAllocate(queryWorkersPool , indexPool , dictionary ,ALLOCATE_STEP ,optimizer.replication);
                    job.highEvictQueueElement.allocate(queryWorkersPool , indexPool, dictionary ,ALLOCATE_STEP ,optimizer.replication);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        public void setStop(){
            stop = true;
        }

        public void addJob(EvictorJob evictorJob){
            try {
                threadQueue.put(evictorJob);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }



    private class EvictorJob{
        public EvictQueueElement highEvictQueueElement;
        public EvictQueueElement lowEvictQueueElement;
        public EvictorJob(EvictQueueElement highEvictQueueElement, EvictQueueElement lowEvictQueueElement){
            this.highEvictQueueElement = highEvictQueueElement;
            this.lowEvictQueueElement = lowEvictQueueElement;
        }
    }

}
