package distiributed;


import QueryStuff.Query;
import triple.TriplePattern2;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;

public class Transporter {

    private final ArrayList<String> hosts;
    private final ArrayList<Sender> senderPool;
    private final ArrayList<Receiver> receiversPool;

    private final HashMap<Integer, ReceiverListener> receiverListenerMap;

    public static int PING_MESSAGE = -999;
    public static int PING_REPLY_MESSAGE = -998;
    public static int QUERY_MSG = -997;
    private final String myIP;

    private final RemoteQueryListener remoteQueryListener;

    private final HashMap<Integer, ArrayList<TriplePattern2>> waitngTriplePatterns;
    private final HashMap<Integer, Query> waitngQueries;
    private final HashMap<Integer, SendItem> tempBuffer;

    public Transporter(ArrayList<String> hosts, RemoteQueryListener remoteQueryListener) {
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
            Receiver receiver = new Receiver(this, hosts.get(i), i);
            receiver.start();
            receiversPool.add(receiver);
        }
        waitngTriplePatterns = new HashMap<>();
        waitngQueries = new HashMap<>();
        tempBuffer = new HashMap<>();
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
            senderPool.get(i).sendQuery(query, queryNo);
        }
    }


    public void receive(int queryNo, ReceiverListener cBack) {
        if (cBack != null)
            receiverListenerMap.put(queryNo, cBack);
        else
            receiverListenerMap.remove(queryNo);
    }

    public void recievedQuery(String query, int queryNo) {
        if (remoteQueryListener != null)
            remoteQueryListener.gotQuery(query, queryNo);
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

    public void gotPingReply(int id) {
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


    public interface ReceiverListener {
        void gotResult(SendItem sendItem);

    }


    public interface RemoteQueryListener {
        void gotQuery(String query, int queryNo);
    }


}
