package distiributed;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SenderThread extends Thread{

    private final static int PORT = 1290;
    private final String host;
    private boolean stop = false;

    private Socket socket ;
    public boolean working = false;

    private BlockingQueue<SendItem> sharedWorkQueue = new ArrayBlockingQueue(10000);

    public SenderThread(String host) {
        this.host = host;
    }


    @Override
    public void run(){
        working = true;
        openSocket();
        while(!stop){
            try {
                working = false;
               SendItem sendItem =  sharedWorkQueue.take();
               if(stop)
                   break;
               working = true;
               send(sendItem);
            } catch (InterruptedException e) {
                e.printStackTrace();
                working = false;
            }
        }
    }

    private void openSocket() {
        try {
            socket = new Socket(host , PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void send(SendItem sendItem){
        try {
            ObjectOutputStream outToServer = new ObjectOutputStream(socket.getOutputStream());
            outToServer.write(sendItem.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
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
