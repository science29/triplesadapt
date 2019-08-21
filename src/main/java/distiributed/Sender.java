package distiributed;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Sender{

    private final static int PORT = 1290;
    private static final int THREAD_COUNT_PER_HOST = 2;
    private final String host;


    private final ArrayList<SenderThread> threads ;

    private BlockingQueue<SendItem> sharedWorkQueue = new ArrayBlockingQueue(10000);

    public Sender(String host) {
        this.host = host;
        this.threads = new ArrayList<>();
        for(int i = 0 ; i < THREAD_COUNT_PER_HOST ; i++){
            SenderThread senderThread = new SenderThread();
            threads.add(senderThread);
            senderThread.start();
        }
    }

    public void addWork(SendItem sendItem) {
        try {
            sharedWorkQueue.put(sendItem);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    private class SenderThread extends Thread {
        private boolean stop = false;
        private Socket socket ;
        private boolean working = false;
        ObjectOutputStream outToServer ;
        boolean socketOpened = false;

        @Override
        public void run() {
            working = true;
            openSocket();
            while (!stop) {
                try {
                    working = false;
                    SendItem sendItem = sharedWorkQueue.take();
                    if (stop)
                        break;
                    if(!socketOpened)
                        openSocket();
                    working = true;
                    send(sendItem);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    working = false;
                }
            }
            closeSocket();
        }

        private void closeSocket() {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void stopWorking() {
            stop = true;
            addWork(new SendItem()); //TODO not a correct way!
        }

        private void openSocket() {
            try {
                socket = new Socket(host, PORT);
                outToServer = new ObjectOutputStream(socket.getOutputStream());
                socketOpened = true;
            } catch (IOException e) {
                e.printStackTrace();
            }catch (Exception e){
                System.err.println("socket to "+host +" is not opened");
            }
        }

      /*  private void send(SendItem sendItem) {
            try {

                ArrayList<Integer> list  = new ArrayList<>();
                sendItem.serialize(list);
                outToServer.writeInt(list.size());
                for(int i =0 ; i < list.size() ; i++){
                    outToServer.writeInt(list.get(i));
                }
                outToServer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/

        private void send(SendItem sendItem) {
            try {
                byte [] data = sendItem.getBytes();
                outToServer.writeInt(data.length);
                outToServer.write(data);
                outToServer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
