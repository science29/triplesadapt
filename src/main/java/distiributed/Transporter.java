package distiributed;


import triple.TriplePattern2;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Transporter {


    private static final int PORT = 1984;
    private final ArrayList<String> hosts;
    private final HashMap<Integer , ArrayList<Socket>> hostsSocket ;
    private final HashMap<Integer , SenderThread> senderPool;
    private final int soketsPerHost = 3;

    public Transporter(ArrayList<String> hosts){
        this.hosts = hosts;
        senderPool = new HashMap<>();
        hostsSocket = new HashMap<>();
        for(int i = 0 ; i < hosts.size() ; i++){
            ArrayList<Socket> sockets = new ArrayList<>();
            for(int j = 0 ; j < soketsPerHost ; j++){
                try {
                    sockets.add(new Socket(hosts.get(i) , PORT));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            hostsSocket.put(i ,sockets );
        }
    }

    public void sendToAll(SendItem sendItem){

    }


    public void receive(int queryNo , TriplePattern2 triplePattern2 , ReceiverListener cBack){

    }



    public interface ReceiverListener{
        void gotResult(SendItem sendItem);

    }
}
