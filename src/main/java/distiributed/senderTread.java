package distiributed;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class senderTread extends Thread{

    private final static int PORT = 1290;
    private boolean stop = false;

    private final ArrayList<String> hosts ;
    private final HashMap<Integer, Socket> hostsSocket;

    private BlockingQueue<SendItem> sharedWorkQueue = new ArrayBlockingQueue(10000);

    public senderTread(ArrayList<String> hosts) {
        this.hosts = hosts;
        this.hostsSocket = new HashMap<>();
    }

    @Override
    public void run(){
        openSockets();
        while(!stop){
            try {
               SendItem sendItem =  sharedWorkQueue.take();
               if(stop)
                   break;
               sendAll(sendItem);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void openSockets() {
        for(int i = 0 ; i < hosts.size() ; i++){
            try {
                Socket clientSocket = new Socket(hosts.get(i), PORT);
                hostsSocket.put(i , clientSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendAll(SendItem sendItem) {
        for(int i = 0 ; i < hosts.size() ; i++){
            Socket socket = hostsSocket.get(i);
            try {
                ObjectOutputStream outToServer = new ObjectOutputStream(socket.getOutputStream());
                outToServer.write(sendItem.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public void addWork(SendItem sendItem){
        try {
            sharedWorkQueue.put(sendItem);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stopWorking(){
        stop = true;
        addWork(new SendItem());
    }
}
