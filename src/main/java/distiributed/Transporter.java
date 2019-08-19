package distiributed;


import triple.TriplePattern2;

import java.util.ArrayList;
import java.util.HashMap;

public class Transporter {


    private static final int PORT = 1984;
    private final ArrayList<String> hosts;
    private final ArrayList<Sender> senderPool;
    private final ArrayList<Receiver> receiversPool;
    private final int soketsPerHost = 3;

    public Transporter(ArrayList<String> hosts){
        this.hosts = hosts;
        senderPool = new ArrayList<>();
        receiversPool = new ArrayList<>();
        for(int i = 0 ; i < hosts.size() ; i++){
            Sender sender = new Sender(hosts.get(i));
            senderPool.add(sender);
            Receiver receiver = new Receiver(receiverListener);
            receiver.start();
            receiversPool.add(receiver);
        }
    }

    public void sendToAll(SendItem sendItem){
        for(int i = 0 ; i < senderPool.size() ; i++){
            senderPool.get(i).addWork(sendItem);
        }
    }


    public void receive(int queryNo , TriplePattern2 triplePattern2 , ReceiverListener cBack){

    }



    public interface ReceiverListener{
         void gotResult(SendItem sendItem);

    }
}
