package distiributed;


import QueryStuff.Query;
import index.MyHashMap;
import optimizer.Optimizer2;
import triple.Triple;
import triple.TriplePattern2;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Set;

public class Transporter implements Receiver.RecieverReadyListener {


    private final ArrayList<String> hosts;
    private final ArrayList<Sender> senderPool;
    private final ArrayList<Receiver> receiversPool;

    private final HashMap<Integer, ReceiverListener> receiverListenerMap;

    public static int PING_MESSAGE = -999;
    public static int PING_REPLY_MESSAGE = -998;
    public static int QUERY_MSG = -997;
    public static int TEST_MESSAGE = -996;
    public static final int TEST_REPLY_MESSAGE = -995;
    public static final int QUERIES_SESSION = -994 ;
    public static final int QUERIES_DONE_BATCH = -993;

    public static final int REPLICATION_REQUEST = -992;

    public static final int SEND_REPLICATION_BACK = -991;

    public static final int FINISHED_SENDING_REPLICATION_ON_DIST = -990;
    public static final int SEND_FULL_INDEX_FINSHED = -989;
    public static final int SEND_FULL_INDEX = -988;

    private final String myIP;

    private final RemoteQueryListener remoteQueryListener;

    private final HashMap<Integer, ArrayList<TriplePattern2>> waitngTriplePatterns;
    private final HashMap<Integer, Query> waitngQueries;
    private final HashMap<Integer, SendItem> tempBuffer;
    private double networkCostMB = -1;
    private int readyReciever = 0 ;
    private TransporterReadyListener transporterReadyListener;

    public void sendShare(MyHashMap <Integer,ArrayList<Triple>> index, byte indexType ,int to) {
        senderPool.get(to).sendFullIndex(index , indexType);
    }


    public interface DataReceivedListener{
        void gotData(SendItem sendItem);
    }

    public interface TransporterReadyListener{
        void ready();
    }


    public int getSendersCount(){
        if(senderPool == null)
            return 0;
        return senderPool.size();
    }



    public Transporter(ArrayList<String> hosts, RemoteQueryListener remoteQueryListener , Optimizer2 optimizer , TransporterReadyListener transporterReadyListener) {
        myIP = removeMySelf(hosts);
        this.remoteQueryListener = remoteQueryListener;
        this.hosts = hosts;
        senderPool = new ArrayList<>();
        receiversPool = new ArrayList<>();
        receiverListenerMap = new HashMap<>();
        // test();
        for (int i = 0; i < hosts.size(); i++) {
            Sender sender = new Sender(hosts.get(i), myIP, this, i);
            senderPool.add(sender);
            Receiver receiver = new Receiver(this, hosts.get(i), i , optimizer, this);
            receiver.start();
            receiversPool.add(receiver);
        }
        waitngTriplePatterns = new HashMap<>();
        waitngQueries = new HashMap<>();
        tempBuffer = new HashMap<>();
        this.transporterReadyListener = transporterReadyListener;
        setSedingCost();
    }


    public double getSendingItemCostMB() {
        return networkCostMB;
    }


    public void setSedingCost(){
        long startTime = System.nanoTime();
        pingListeners.put(984721, new Sender.PingListener() {
            @Override
            public void onPingReply() {
                long stopTime = System.nanoTime();;
                long elapsedTimeS = (stopTime - startTime) / 1000;
                networkCostMB = elapsedTimeS;
                checkIfReady();
            }
        });
        senderPool.get(0).sendTestMesg(1000000);

    }

    private String removeMySelf(ArrayList<String> hosts) {

        Enumeration e = null;
        try {
            e = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e1) {
            e1.printStackTrace();
        }
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress idd = (InetAddress) ee.nextElement();
                for (int i = 0; i < hosts.size(); i++) {
                    if (idd.getHostAddress().matches(hosts.get(i))) {
                        String my = hosts.remove(i);
                        return my;
                    }
                }
            }
        }
        System.err.println("the IP is not correctly set!");
        return null;
    }

    public void sendToAll(SendItem sendItem) {
        if(sendItem == null)
            return;
        for (int i = 0; i < senderPool.size(); i++) {
            senderPool.get(i).addWork(sendItem);
        }
    }

    public void sendQuery(String query, int queryNo) {
        for (int i = 0; i < senderPool.size(); i++) {
            senderPool.get(i).sendQuery(query, queryNo , true);
        }
    }

    public void sendManyQueires(ArrayList<String> list , ArrayList<Integer> queryNos){
        ArrayList<Integer>[] shareNos = new ArrayList[senderPool.size()];
        ArrayList<String>[] shareQueries = new ArrayList[senderPool.size()];
        for(int j = 0 ; j < senderPool.size() ; j++){
            if(shareNos[j] == null) {
                shareNos[j] = new ArrayList<>();
                shareQueries[j] = new ArrayList<>();
            }
            for(int i = list.size()-1 ; i >= 0 ; i--){
                int senderIndex = (i+j)%senderPool.size();
                shareNos[senderIndex].add(queryNos.get(i));
                shareQueries[senderIndex].add(list.get(i));
            }
            for (int i = 0; i < senderPool.size(); i++) {
                senderPool.get(i).sendQueryList(shareNos[i] , shareQueries[i]);
            }
        }



        /*for(int j = 0 ; j < senderPool.size() ; j++)
            for(int i = list.size() ; i >= 0 ; i--){
                senderPool.get((i+j)%senderPool.size()).sendQuery(list.get(i), queryNos.get(i) , i == 0);
            }*/
    }


    public void informDoneQuery(Set<Integer> queriesNos){
        for(int i = 0; i < senderPool.size() ; i++){
            senderPool.get(i).sendQueryListDone(new ArrayList<>(queriesNos));
        }
    }


    public void receive(int queryNo, ReceiverListener cBack) {
        if (cBack != null)
            receiverListenerMap.put(queryNo, cBack);
        else
            receiverListenerMap.remove(queryNo);
    }

    public void recievedQuery(String query, int queryNo , int batchCount , int batchID) {
        if (remoteQueryListener != null)
            remoteQueryListener.gotQuery(query, queryNo , batchCount , batchID);
    }

    public void receive(TriplePattern2 triplePattern2, int queryNo) {
        ArrayList<TriplePattern2> list = waitngTriplePatterns.get(queryNo);
        if (list == null)
            list = new ArrayList<>();
        list.add(triplePattern2);
    }

    public void receive(Query query) {
        waitngQueries.put(query.ID, query);
        if (tempBuffer.containsKey(query.ID)) {
            receiverGotResult(tempBuffer.get(query.ID));
        }
    }

    public void recievedQueryDone(int queryNo) {
        remoteQueryListener.queryDone(queryNo);
    }




    public void receiverGotResult(SendItem sendItem) {
        boolean forwarded = false;
        //TODO remove the listener if finished
        ReceiverListener listener = receiverListenerMap.get(sendItem.queryNo);
        if (listener != null)
            listener.gotResult(sendItem);
        ArrayList<TriplePattern2> list = waitngTriplePatterns.get(sendItem.queryNo);
        if (list != null)
            for (int i = 0; i < list.size(); i++) {
                list.get(i).gotRemoteBorderResult(sendItem);
                forwarded = true;
            }

        Query query = waitngQueries.get(sendItem.queryNo);
        if (query != null) {
            query.gotRemoteResult(sendItem);
            forwarded = true;
        }
        if (!forwarded)
            tempBuffer.put(sendItem.queryNo, sendItem);
    }

    public void pingBack(int hostID) {
        senderPool.get(hostID).pingBack();
    }


    private int numberOFTestMessageToRecieve = 100;
    private int currentTestCount = 0;

    public void replyTestMssg(byte[] data , int hostID) {
        currentTestCount++;
        if(currentTestCount > numberOFTestMessageToRecieve)
            senderPool.get(hostID).sendTestMesgBack();
    }

    public void gotPingReply(int id) {
        Sender.PingListener pingListener = pingListeners.get(id);
        pingListener.onPingReply();
    }

    public void gotTestReplyMsg(int id) {
        Sender.PingListener pingListener = pingListeners.get(id);
        pingListener.onPingReply();
    }


    HashMap<Integer, Sender.PingListener> pingListeners = new HashMap<>();

    public void listenForPingReply(int hostID, Sender.PingListener pingListener) {
        pingListeners.put(hostID, pingListener);
    }


    public void printSummary() {
        System.out.println("printing transport summary");
        for (int i = 0; i < senderPool.size(); i++) {
            if (senderPool.get(i).connectionMsg != null) {
                System.out.println(i + "/" + senderPool.size() + ":" + senderPool.get(i).connectionMsg);
            }
        }
    }

    public String getHost() {
        return myIP;
    }




    public void getReplication(int currentWorkingNode, int currentTargetedDistance, int from , int to ,
                               DataReceivedListener dataReceivedListener){

        senderPool.get(currentWorkingNode).sendFullReplicationRequst(currentTargetedDistance, from, to, dataReceivedListener);

    }

    public void setReplicationListener(int hostID, DataReceivedListener dataReceivedListener) {
        receiversPool.get(hostID).replicationListener = dataReceivedListener;
    }


    public void sendReplicationBack(int toId, ArrayList<Triple> res , boolean finished) {
        senderPool.get(toId).sendFullReplicationBack(res , finished);
    }


    @Override
    public synchronized void recieverReady(int id) {
        readyReciever++;
        if(readyReciever >= receiversPool.size()){
            checkIfReady();
        }
    }

    private void checkIfReady() {
        //check the recievers
        if(readyReciever < receiversPool.size())
            return;
        //check the network cost
        if(networkCostMB < 0)
            return;

        if(transporterReadyListener != null)
            transporterReadyListener.ready();
    }


    public interface ReceiverListener {
        void gotResult(SendItem sendItem);

    }


    public interface RemoteQueryListener {
        void gotQuery(String query, int queryNo , int count , int batchID);
        void queryDone(int queryNo);
    }


}
