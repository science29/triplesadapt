package distiributed;


import triple.TriplePattern2;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

public class Transporter {


    private static final int PORT = 1984;
    private final ArrayList<String> hosts;
    private final ArrayList<Sender> senderPool;
    private final ArrayList<Receiver> receiversPool;
    private final int soketsPerHost = 3;
    private final HashMap<Integer , ReceiverListener> receiverListenerMap;

    public static int PING_MESSAGE = -999;
    public static int PING_REPLY_MESSAGE = -998;



    public Transporter(ArrayList<String> hosts){
        String myIP = removeMySelf(hosts);
        this.hosts = hosts;
        senderPool = new ArrayList<>();
        receiversPool = new ArrayList<>();
        receiverListenerMap = new HashMap<>();
        for(int i = 0 ; i < hosts.size() ; i++){
            Sender sender = new Sender(hosts.get(i) , myIP , this,i);
            senderPool.add(sender);
            Receiver receiver = new Receiver(this , hosts.get(i) , i);
            receiver.start();
            receiversPool.add(receiver);
        }
    }


    private String removeMySelf(ArrayList<String> hosts) {
        InetAddress ip = null;
        String hostname;
        try {
            ip = InetAddress.getLocalHost();
            // hostname = ip.getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        for(int i = 0 ; i <  hosts.size() ; i++){
            String my = ip.toString();
            if(my.matches(hosts.get(i))) {
                hosts.remove(i);
                return ip.toString();
            }
            return ip.toString();
        }

        return ip.toString();
    }

    public void sendToAll(SendItem sendItem){
        for(int i = 0 ; i < senderPool.size() ; i++){
            senderPool.get(i).addWork(sendItem);
        }
    }


    public void receive(int queryNo ,  ReceiverListener cBack){
        if(cBack != null)
            receiverListenerMap.put(queryNo , cBack);
        else
            receiverListenerMap.remove(queryNo);
    }



    public void receiverGotResult(SendItem sendItem) {
        ReceiverListener listener = receiverListenerMap.get(sendItem.queryNo);
        if(listener != null)
            listener.gotResult(sendItem);
    }

    public void pingBack(int hostID) {
        senderPool.get(hostID).ping();
    }

    public void gotPingReply(int id) {
        Sender.PingListener pingListener = pingListeners.get(id);
        pingListener.onPingReply();
    }


    HashMap<Integer , Sender.PingListener> pingListeners = new HashMap<>();

    public void listenForPingReply(int hostID , Sender.PingListener pingListener) {
        pingListeners.put(hostID , pingListener);
    }


    public void printSummary(){
        System.out.println("printing transport summary");
        for(int i = 0 ; i < senderPool.size() ; i++){
            if(senderPool.get(i).connectionMsg != null){
                System.out.println(i+"/"+senderPool.size()+":"+ senderPool.get(i).connectionMsg);
            }
        }
    }

    public interface ReceiverListener{
         void gotResult(SendItem sendItem);

    }
}
