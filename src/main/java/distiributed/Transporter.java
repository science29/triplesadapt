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

    public Transporter(ArrayList<String> hosts){

        hosts = new ArrayList<>();
        hosts.add("172.20.32.8");
        hosts.add("172.20.32.7");
        removeMySelf(hosts);
        this.hosts = hosts;
        senderPool = new ArrayList<>();
        receiversPool = new ArrayList<>();
        receiverListenerMap = new HashMap<>();
        for(int i = 0 ; i < hosts.size() ; i++){
            Sender sender = new Sender(hosts.get(i));
            senderPool.add(sender);
            Receiver receiver = new Receiver(this);
            receiver.start();
            receiversPool.add(receiver);
        }
    }

    private void removeMySelf(ArrayList<String> hosts) {
        for(int i = 0 ; i <  hosts.size() ; i++){


            InetAddress ip;
            String hostname;
            try {
                ip = InetAddress.getLocalHost();
               // hostname = ip.getHostName();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                return;
            }
            String my = ip.toString();
            if(my.matches(hosts.get(i))) {
                hosts.remove(i);
                return;
            }
        }
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


    public interface ReceiverListener{
         void gotResult(SendItem sendItem);

    }
}
