package optimizer.Replication;

import distiributed.SendItem;
import distiributed.Transporter;
import index.IndexesPool;
import optimizer.EngineRotater2;
import optimizer.Rules.GeneralReplicationInfo;
import triple.Triple;

import java.util.*;

public class Replication {

    private final EngineRotater2 optimizer;
    Transporter transporter ;
    FullReplicationManger fullReplicationManger;
    private final HashMap<Integer,Boolean> borderTripleMap;
    private final IndexesPool indexesPool;

    public final static int SINGLE_REPLICATION_STEP_SIZE = 10000;

    GeneralReplicationInfo generalReplicationInfo;




    public interface FullReplicationListener{
        void done();
    }

    public Replication(Transporter transporter, HashMap<Integer, Boolean> borderTripleMap, IndexesPool indexesPool , EngineRotater2 optimizer) {
        this.transporter = transporter;
        this.borderTripleMap = borderTripleMap;
        this.indexesPool = indexesPool;
        this.optimizer = optimizer;
        this.generalReplicationInfo = optimizer.genralReplicationInfo;
        this.fullReplicationManger = new FullReplicationManger();
    }


    public void performFullReplication(int distance){
        if(fullReplicationManger.requiredDistance >= distance)
            return;
        fullReplicationManger.perform(distance , Integer.MAX_VALUE );
    }


    public void performStepReplication(int distance , int tripleCount){
        if(fullReplicationManger.requiredDistance >= distance)
            return;
        fullReplicationManger.perform(distance , tripleCount  );
    }


    public void performOneStepReplication() {
        fullReplicationManger.performStep();
    }

    public void backOneStepReplication() {
        fullReplicationManger.backOneStep();
    }


    public void sendRequiredReplications(int toId, int dist, int from, int to) {
        fullReplicationManger.sendRequiredReplications(toId , dist , from , to);
    }

    private void putListInIndex(ArrayList<Triple> list) {
       // byte indexType = optimizer.getReplicationDestiIndexType();
        for(int i = 0 ; i < list.size() ; i++){
            indexesPool.addToReplication(list.get(i));
        }
    }



    private class FullReplicationManger{
        final int STEP = SINGLE_REPLICATION_STEP_SIZE;

       // int replicationStep.distance = 0;
       // int replicationStep.nodeNumber = 0 ;
       // int replicationStep.tripleCountWithinNode = 0;
        int requiredDistance = 0;
        int requiredTripleCount = 0;
        boolean working = false;

        HashMap<Integer, HashMap<Integer , Integer> > distanceVerticesMap; //Map of distance , then on vertex
        Integer BFS_StartingVertex;

        private ReplicationStep replicationStep;

        public FullReplicationManger(){
            initialDistanceMap();
            intialReplication();
        }

        //TODO fix this
        public void performStep(){
            perform(requiredDistance , requiredTripleCount + STEP);
        }


        public void perform(int distance , int tripleCount){
            if(distance <= requiredDistance || replicationStep.tripleCountWithinNode > tripleCount)
                return;
            if(working){
                requiredDistance = distance;
                requiredTripleCount = tripleCount;
                return;
            }else{
                replicationStep.distance++;
            }
            intialDistanceLevel();
            work();
        }



        public void intialReplication(){
            if(generalReplicationInfo.currentStep == null )
                generalReplicationInfo.currentStep = new ReplicationStep(0,0,0);
            replicationStep = generalReplicationInfo.currentStep;
        }

        private void intialNodeLevel(){
            replicationStep.tripleCountWithinNode = 0;
        }

        private void intialDistanceLevel(){
            intialNodeLevel();
            replicationStep.nodeNumber = 0;
            replicationStep.tripleCountWithinDistance = 0;
        }


        //will stop when the replicationStep.distance > requiredDistance
        private void work(){
            if(replicationStep.distance  > requiredDistance || replicationStep.tripleCountWithinDistance > requiredTripleCount) {
                working = false;
                return;
            }
            working = true;
            int from = replicationStep.tripleCountWithinNode;
            int to = from + STEP;
            replicationStep.tripleCountWithinNode += STEP;
            replicationStep.tripleCountWithinDistance += STEP;
           /* if(replicationStep.nodeNumber == myNodeNmber) {
                replicationStep.nodeNumber++;
                intialNodeLevel();
            }*/
            if(replicationStep.nodeNumber >= transporter.getSendersCount()) {
                replicationStep.distance++;
                intialDistanceLevel();
                work();
                return;
            }
            transporter.getReplication(replicationStep.nodeNumber, replicationStep.distance, from , to , new Transporter.DataReceivedListener() {
                @Override
                public void gotData(SendItem sendItem) {
                    ArrayList<Triple> list =  sendItem.toTripleList();
                    if(list != null)
                        putListInIndex(list);
                    if(sendItem.queryNo == Transporter.FINISHED_SENDING_REPLICATION_ON_DIST) {
                        replicationStep.nodeNumber++;
                        intialNodeLevel();
                    }
                    work();
                    return;
                }
            });
        }

        int currentDoneBFSDist = 0;
        public void increaseBFSDistance(){
            HashMap<Integer, Integer> map = distanceVerticesMap.get(currentDoneBFSDist);
            HashMap<Integer , Integer> newVertxDistMap = new HashMap<>();
            distanceVerticesMap.put((currentDoneBFSDist+1) , newVertxDistMap);
            Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()){
                Integer v = iterator.next().getKey();
                ArrayList<Triple> list = indexesPool.getTriples(v);
                for(int i = 0 ; i < list.size() ; i++){
                    if(list.get(i).triples[0] == v ){
                        if(!isAlreadySeenBFS(list.get(i).triples[0] , 0)) {
                            newVertxDistMap.put(list.get(i).triples[0], map.get(v) + 1);
                        }
                    }else {
                        if(!isAlreadySeenBFS(list.get(i).triples[2] , 0)) {
                            newVertxDistMap.put(list.get(i).triples[2], map.get(v) + 1);
                        }
                    }
                }
            }

        }

        private boolean isAlreadySeenBFS(int v , int distance) {
            HashMap<Integer, Integer> map = distanceVerticesMap.get(distance);
            if(map == null)
                return false;
            if(map.containsKey(v))
                return true;
            else return isAlreadySeenBFS(v , distance+1);
        }


        public void initialDistanceMap(){
            distanceVerticesMap = new HashMap<>();
            HashMap<Integer , Integer> vertexDistanceMap = new HashMap<>();
            Iterator<Map.Entry<Integer, Boolean>> iterator = borderTripleMap.entrySet().iterator();
            while (iterator.hasNext()){
                Integer v = iterator.next().getKey();
                vertexDistanceMap.put(v,0); //TODO 0 or 1
            }
            distanceVerticesMap.put( 0 ,vertexDistanceMap);
        }

        /*public ArrayList<Triple> getAtDist(Integer v , int requiredDistance , int currentDistance){
            xx;
        }*/

        public void sendRequiredReplications(int toId, int dist, int from, int to) {
            //get the required replication
            while(!distanceVerticesMap.containsKey(dist))
                increaseBFSDistance();
            Iterator<Map.Entry<Integer, Integer>> iterator = distanceVerticesMap.get(dist).entrySet().iterator();
            int i = 0;
            ArrayList<Triple> res = null;
            while (iterator.hasNext() && (res == null || res.size() < to)){
                if(i < from){
                    i++;
                    iterator.next();
                    continue;
                }
                Integer v = iterator.next().getKey();
                ArrayList<Triple> list = indexesPool.getTriples(v);
                if(res == null)
                    res = list;
                else{
                    res.addAll(list);
                }

            }
            //send it
            boolean finished = (res == null || res.size() < to);
            transporter.sendReplicationBack(toId , res , finished);
        }

        public void backOneStep() {
           // xx;
            //TODO:
        }
    }


}
